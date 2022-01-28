package cacheaware

import (
	"context"
	"strings"

	client "github.com/datashim-io/datashim/src/apiclient/pkg/clientset/versioned"
	datashim "github.com/datashim-io/datashim/src/dataset-operator/pkg/apis/com/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	klog "k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	Name                     = "CacheAwarePlugin"
	ErrReasonNoCacheDeployed = "Node does not have cache deployment for pod"
	hostTopologyKey          = "kubernetes.io/hostname"
)

var _ framework.FilterPlugin = &CacheAwarePlugin{}
var _ framework.ScorePlugin = &CacheAwarePlugin{}

type CacheAwarePlugin struct {
	handle framework.Handle
	client *client.Clientset
}

func NewCacheAwarePlugin(obj runtime.Object, h framework.Handle) (framework.Plugin, error) {

	klog.V(3).Infof("Initialising the cache-aware plugin")
	//args, ok := obj.(*config.CacheAwareArgs)
	//if !ok {
	//	return nil, fmt.Errorf("want args to be of type CacheAwareArgs, got %T", obj)
	//}
	//conf, err := clientcmd.BuildConfigFromFlags(args.KubeMaster, args.KubeConfig)
	//if err != nil {
	//	klog.V(3).ErrorS(err, "Could not create instance for cache-aware plugin", "kube-master", args.KubeMaster)
	//	return nil, err
	//}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		klog.V(3).Error(err, "Could not create instance for cache-aware plugin")
		return nil, err
	}

	client, err := client.NewForConfig(cfg)

	if err != nil {
		klog.V(3).Error(errors.NewInternalError(err), "Failed to set up connection to Datashim")
		return nil, err
	}

	return &CacheAwarePlugin{handle: h, client: client}, nil
}

func (f *CacheAwarePlugin) Name() string {
	return Name
}

func (f *CacheAwarePlugin) fetchDatasetLabelsForPod(pod *v1.Pod) []string {

	var datasets = make([]string, 0)
	podLabels := pod.ObjectMeta.Labels

	for key, value := range podLabels {
		if strings.Contains(key, "dataset") && strings.Contains(key, "id") {
			//Get the dataset object
			//Find if there are any gateways or databackends
			//Filter out any nodes that are not in the same zone as gateways
			datasets = append(datasets, value)
			klog.V(3).Infof("pod %s has dataset %s", pod.ObjectMeta.Name, value)
		}
	}
	return datasets
}

func (f *CacheAwarePlugin) fetchDatasetObjects(ctx context.Context, datasetName string, namespace string) (*datashim.Dataset, *datashim.DatasetInternal, error) {

	//dataset := &datashim.Dataset{}
	//datasetInternal := &datashim.DatasetInternal{}

	dataset, err := f.client.ComV1alpha1().Datasets(namespace).Get(ctx, datasetName, metav1.GetOptions{})

	if err != nil {
		klog.V(1).InfoS("Could not retrieve dataset from the apiserver", "dataset", datasetName, "error ", err)
		return nil, nil, err
	}

	datasetInternal, err := f.client.ComV1alpha1().DatasetInternal(namespace).Get(ctx, datasetName, metav1.GetOptions{})

	if err != nil {
		klog.V(1).InfoS("Could not retrieve datasetInternal for dataset from the apiserver", "datasetInternal", datasetName, "error", err)
		return dataset, nil, err
	}

	return dataset, datasetInternal, nil

}

func (f *CacheAwarePlugin) fetchDeploymentforDataset(d *datashim.DatasetInternal) (gateways []string, datalocs []string) {

	if d.Status.Caching.Placements.Gateways == nil && d.Status.Caching.Placements.DataLocations == nil {
		return nil, nil
	}

	var gw = []string{}
	var dl = []string{}

	if d.Status.Caching.Placements.Gateways != nil {
		for _, g := range d.Status.Caching.Placements.Gateways {
			//We are assuming here that the key will *always* be kubernetes.io/hostname
			if g.Key == hostTopologyKey {
				gw = append(gw, g.Value)
			}
		}
	}

	if d.Status.Caching.Placements.DataLocations != nil {
		for _, d := range d.Status.Caching.Placements.DataLocations {
			if d.Key == hostTopologyKey {
				dl = append(dl, d.Value)
			}
		}
	}

	return gw, dl
}

func (f *CacheAwarePlugin) fetchAllGatewaysDataLocations(ctx context.Context, datasetLabels []string, podNs string) (gateways []*framework.NodeInfo,
	dataLocations []*framework.NodeInfo) {

	var gWays, dLocs []*framework.NodeInfo

	for _, dataset := range datasetLabels {
		ds, dsi, err := f.fetchDatasetObjects(ctx, dataset, podNs)

		if err != nil {
			klog.V(3).Infof("Could not retrieve information for dataset %s", dataset)
			continue
		}

		klog.V(3).Infof("dataset %s has Caching Status %s because of Info %s", ds.Name, ds.Status.Caching.Status, ds.Status.Caching.Info)

		if ds.Status.Provision.Status != datashim.StatusOK {
			//Dataset has not been provisioned yet, cannot make any decisions
			klog.V(3).Infof("Dataset %s is not provisioned yet or is in error", ds.Name)
			return nil, nil
		}

		if ds.Status.Caching.Status == datashim.StatusDisabled {
			//Nothing to see here, move along as there is no caching
			klog.V(3).Infof("dataset %s is not cached and there is no scheduling decision to be made", ds.Name)
			return nil, nil
		}

		// We are accepting Pending status on the cache as the ceph-cache impl. doesn't seem to update this status once
		// the gateway is created ?
		if ds.Status.Caching.Status == datashim.StatusOK || ds.Status.Caching.Status == datashim.StatusPending {
			klog.V(3).Infof("dataset %s is cached.. fetching deployment information", ds.Name)
			gw, dl := f.fetchDeploymentforDataset(dsi)

			if len(gw) == 0 {
				klog.V(3).Infof("No Gateways available for cache deployment of dataset %s", ds.Name)

			} else {
				klog.V(3).Infof("Gateway list for dataset %s is %v", ds.Name, gw)
				for _, g := range gw {
					gNode, err := f.handle.SnapshotSharedLister().NodeInfos().Get(g)
					if err != nil {
						klog.V(2).Error(err, "Could not find a node matching gateway", g)
					} else {
						gWays = append(gWays, gNode)
					}
				}
			}

			if len(dl) == 0 {
				klog.V(3).Infof("No data locations available for cache deployment of dataset %s", ds.Name)
			} else {
				klog.V(3).Infof("Data locations list for dataset %s is %v", ds.Name, dl)
				for _, d := range dl {
					dlNode, err := f.handle.SnapshotSharedLister().NodeInfos().Get(d)
					if err != nil {
						klog.V(2).Error(err, "Could not find a node matching gateway", d)
					} else {
						dLocs = append(dLocs, dlNode)
					}
				}
			}
		}

	}

	return gWays, dLocs
}

//
//func (f *CacheAwarePlugin) fetchAllDatasets() (*[]datashim.Dataset, *[]datashim.DatasetInternal, error) {
//
//	return nil, nil, nil
//}
