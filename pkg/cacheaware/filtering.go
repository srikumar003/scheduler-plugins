package cacheaware

import (
	"context"

	v1 "k8s.io/api/core/v1"
	klog "k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
)

var _ framework.FilterPlugin = &CacheAwarePlugin{}

//Filter extension invoked at the filter extension point
//We will filter out all the nodes that are not in data locations
//We expect this to be used for node affinity where the cache is only available on a set of nodes as a filesystem
func (f *CacheAwarePlugin) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, node *framework.NodeInfo) *framework.Status {

	datasetLabels := f.fetchDatasetLabelsForPod(pod)

	if len(datasetLabels) == 0 {
		klog.V(3).Infof(" Pod %s has no datasets, not applying filter", pod.Name)
		return nil
	}

	//If we are doing this for *every* node, this will be a slooow filter
	//TODO: Cache dataset information and only check if the cache is still current when
	// a pod comes in
	gws, dls := f.fetchAllGatewaysDataLocations(ctx, datasetLabels, pod.Namespace)

	if len(gws) != 0 {
		klog.V(3).Infof("Data access is mediated through cache gateway and the filter is not applicable to pod %s", pod.Name)
		return nil
	}

	if len(dls) == 0 {
		klog.V(3).Infof("No cache locations in the cluster for datasets of Pod %s, not applying filter", pod.Name)
		return nil
	}

	//In this first implementation, we will scan through the list and check for match
	//TODO: cache dataset info and use the cache to get matches

	found := false

	for _, d := range dls {
		if d.Node().Labels["kubernetes.io/hostname"] == node.Node().Labels["kubernetes.io/hostname"] {
			found = true
		}
	}

	if !found {
		klog.V(3).InfoS("No cache deployed on node", node.Node().Name, "for pod", pod.Name)
		return framework.NewStatus(framework.Unschedulable, ErrReasonNoCacheDeployed)
	}

	klog.V(3).InfoS("Selected node", node.Node().Name, "for pod", pod.Name)

	return nil
}
