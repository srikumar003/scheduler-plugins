package cacheaware

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"

	fakeclient "github.com/datashim-io/datashim/src/apiclient/pkg/clientset/versioned/fake"
	datashim "github.com/datashim-io/datashim/src/dataset-operator/pkg/apis/com/v1alpha1"
)

func makeDataset(datasetName string, datasetNamespace string, gatewayLocations []string, dataLocations []string) (*datashim.Dataset, *datashim.DatasetInternal) {

	newDataset := datashim.Dataset{
		ObjectMeta: metav1.ObjectMeta{
			Name:      datasetName,
			Namespace: datasetNamespace,
		},
		Spec: datashim.DatasetSpec{
			Local:   map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"},
			Type:    "COS",
			Extract: "false",
		},
	}

	gatewayPlacements := []datashim.CachingPlacementInfo{}
	dataPlacements := []datashim.CachingPlacementInfo{}

	for _, gw := range gatewayLocations {
		gatewayPlacements = append(gatewayPlacements, datashim.CachingPlacementInfo{Key: hostTopologyKey, Value: gw})
	}
	for _, dl := range dataLocations {
		dataPlacements = append(dataPlacements, datashim.CachingPlacementInfo{Key: hostTopologyKey, Value: dl})
	}

	newDatasetStatus := datashim.DatasetInternalStatus{
		Caching: datashim.DatasetInternalStatusCaching{
			Placements: datashim.CachingPlacement{
				Gateways:      gatewayPlacements,
				DataLocations: dataPlacements,
			},
		},
	}

	newDatasetInternal := datashim.DatasetInternal{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name:      datasetName,
			Namespace: datasetNamespace,
		},
		Spec: datashim.DatasetSpec{
			Local:   map[string]string{},
			Remote:  map[string]string{},
			Type:    "",
			Url:     "",
			Format:  "",
			Extract: "",
		},
		Status: newDatasetStatus,
	}

	return &newDataset, &newDatasetInternal

}

func makePod(podName string, podNamespace string, datasetName string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: podNamespace,
			Labels: map[string]string{
				"dataset.0.id":    datasetName,
				"dataset.0.useas": "mount",
			},
		},
		Spec: v1.PodSpec{},
	}
}

func makeAllocatableResources(mCPU, RAM, storage int64) v1.ResourceList {

	return v1.ResourceList{
		v1.ResourceCPU:     *resource.NewMilliQuantity(mCPU, resource.DecimalSI),
		v1.ResourceMemory:  *resource.NewQuantity(RAM, resource.BinarySI),
		v1.ResourceStorage: *resource.NewQuantity(storage, resource.BinarySI),
	}

}

func makeNode(hostname string, mCPU int64, RAM int64, disk int64) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: hostname,
			Labels: map[string]string{
				"kubernetes.io/hostname": hostname,
			},
		},
		Spec: v1.NodeSpec{},
		Status: v1.NodeStatus{
			Allocatable: makeAllocatableResources(mCPU, RAM, disk),
		},
	}
}

func TestUtilityFunctions(t *testing.T) {

	ctx := context.Background()
	//state := framework.NewCycleState()
	rand.Seed(time.Now().UnixNano())

	namespace := "fake"
	numNodes := 5
	numDataset := 1
	client := fakeclient.NewSimpleClientset()

	for i := 0; i < numNodes; i++ {
		node := makeNode("node"+strconv.Itoa(i), 1000, 10, 10)
		nodeInfo := framework.NewNodeInfo()
		nodeInfo.SetNode(node)
	}

	var pods []*v1.Pod

	for d := 0; d < numDataset; d++ {
		//We'll initialise each dataset with a single gateway location
		// and 2 data locations
		locInts := rand.Perm(numNodes + 1)
		dataset, datasetInternal := makeDataset("dataset"+strconv.Itoa(d), namespace, []string{"node" + strconv.Itoa(locInts[0])}, []string{"node" + strconv.Itoa(locInts[1]), "node" + strconv.Itoa(locInts[2])})

		var _, err = client.ComV1alpha1().Datasets(namespace).Create(ctx, dataset, metav1.CreateOptions{})

		if err != nil {
			t.Errorf("Could not create dataset instance: %v", err)

		}

		_, err = client.ComV1alpha1().DatasetInternal(namespace).Create(ctx, datasetInternal, metav1.CreateOptions{})

		if err != nil {
			t.Errorf("Could not create datasetinternal instance: %v", err)
		}

		pods = append(pods, makePod("pod"+strconv.Itoa(d), namespace, "dataset"+strconv.Itoa(d)))
	}

	//for _, p := range pods {
	//	datasetLabels = fetchDatasetsForPod()
	//}

}
