package cacheaware

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	klog "k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

type priorityWeight struct {
	cacheAffinityWeight    int
	neighbourWeight        int
	balancedResourceWeight int
}

//TODO (srikumarv) Update this to get the weights from the arguments
func calculateWeight() priorityWeight {

	// Initial values for weights
	weight := priorityWeight{
		cacheAffinityWeight:    2,
		neighbourWeight:        1,
		balancedResourceWeight: 1,
	}

	return weight
}

const (
	// PluginName indicates name of volcano scheduler plugin.
	PluginName = "datacache"

	//CacheAffinityWeight is the key for providing CacheNode Priority Weight in the YAML
	CacheAffinityWeight = "cacheaffinity.weight"

	//NeighbourWeight is the key for providing Neighbour Priority Weight in the YAML
	NeighbourWeight = "neighbour.weight"

	// BalancedResourceWeight is the key for providing Balanced Resource Priority Weight in YAML
	BalancedResourceWeight = "balancedresource.weight"

	HostnameLabel = "kubernetes.io/hostname"
	ZoneLabel     = "topology.kubernetes.io/region"
	RegionLabel   = "topology.kubernetes.io/zone"
)

func (f *CacheAwarePlugin) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

func (f *CacheAwarePlugin) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {

	weights := calculateWeight()

	node, err := f.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)

	if err != nil {
		klog.V(3).Error(err, "could not retrieve information for node", nodeName)
		return 0, framework.AsStatus((fmt.Errorf("could not get information for node %s", nodeName)))
	}

	datasetLabels := f.fetchDatasetLabelsForPod(pod)

	if len(datasetLabels) == 0 {
		klog.V(3).Infof("there is no datasets for Pod %s")
		return 0, framework.AsStatus(fmt.Errorf("there are no datasets for Pod %s", pod.Name))
	}

	gws, dls := f.fetchAllGatewaysDataLocations(ctx, datasetLabels, pod.Namespace)

	if len(gws) == 0 && len(dls) == 0 {
		klog.V(3).Infof("None of the datasets for Pod %s are cached in the cluster", pod.Name)
		return 0, nil
	}

	var score int64 = 0

	if len(dls) != 0 {
		// a false for the above condition would be unusual but
		// it is possible to have a gateway with the storage off-cluster
		// if there are data locations in the cluster, then we lower their score
		// to reduce the chances of double bandwidth hit (up-and-down)
		for _, dl := range dls {
			if topologyLabelMatch(dl, node, HostnameLabel) {
				if len(gws) == 0 {
					// There are no gateways so this cache is configured as a DFS
					// Hopefully, we have used the filter to *only* select the
					// But, let's set the score of the data locations to arbitrarily high
					// we are assuming that the nodeName maps to hostname
					score = int64(framework.MaxNodeScore)
				} else {
					score = 0
				}
			}
		}
	}

	for _, g := range gws {
		if topologyLabelMatch(node, g, HostnameLabel) {
			klog.V(3).Infof("Node %s is sames as cache gateway %s for task pod %s", node.Node().Name, g.Node().Name, pod.Name)
			score = int64(int64(weights.cacheAffinityWeight) * (framework.MaxNodeScore - framework.MinNodeScore) / 2)
		} else if topologyLabelMatch(node, g, RegionLabel) && topologyLabelMatch(node, g, ZoneLabel) {
			score = int64(int64(weights.neighbourWeight) * (framework.MaxNodeScore - framework.MinNodeScore) / 2)
		} else {
			score = 0
		}
	}

	klog.V(3).Infof("Total Score for task %s/%s on node %s is: %f", pod.Namespace, pod.Name, node.Node().Name, score)
	return score, nil
}

func topologyLabelMatch(nodeA *framework.NodeInfo, nodeB *framework.NodeInfo, label string) bool {
	valA, apresent := nodeA.Node().Labels[label]
	valB, bpresent := nodeB.Node().Labels[label]
	if (apresent && bpresent) && (valA == valB) {
		return true
	}
	return false
}
