// Copyright 2024 The Kube-burner Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package burner

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	kubevirtV1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"

	"github.com/kube-burner/kube-burner/pkg/config"
	"github.com/kube-burner/kube-burner/pkg/util"
)

var supportedOps = map[config.KubeVirtOpType]struct{}{
	config.KubeVirtOpStart:   {},
	config.KubeVirtOpStop:    {},
	config.KubeVirtOpRestart: {},
	config.KubeVirtOpPause:   {},
	config.KubeVirtOpUnpause: {},
}

func setupKubeVirtJob(jobConfig config.Job) Executor {
	var ex Executor
	mapper := newRESTMapper()
	for _, o := range jobConfig.Objects {
		if len(o.LabelSelector) == 0 {
			log.Fatalf("Empty labelSelectors not allowed with: %s", o.Kind)
		}
		if len(o.KubeVirtOp) == 0 {
			log.Fatalln("Empty Patch Type not allowed")
		}
		if _, ok := supportedOps[o.KubeVirtOp]; !ok {
			log.Fatalf("Unsupported KubeVirtOp: %s", o.KubeVirtOp)
		}

		apiVersion := "kubevirt.io/v1"
		if len(o.APIVersion) != 0 {
			apiVersion = o.APIVersion
		}

		kind := "VirtualMachine"
		if len(o.Kind) != 0 {
			kind = o.Kind
		}

		gvk := schema.FromAPIVersionAndKind(apiVersion, kind)
		mapping, err := mapper.RESTMapping(gvk.GroupKind())
		if err != nil {
			log.Fatal(err)
		}

		obj := object{
			gvr:    mapping.Resource,
			Object: o,
		}
		ex.objects = append(ex.objects, obj)
	}
	return ex
}

func (ex *Executor) RunKubeVirtJob(restConfig *rest.Config) {
	var itemList *unstructured.UnstructuredList
	var wg sync.WaitGroup

	virtClient, err := kubecli.GetKubevirtClientFromRESTConfig(restConfig)
	if err != nil {
		log.Error("Failed to create VirtCTL Client", err)
	}

	for _, obj := range ex.objects {

		labelSelector := labels.Set(obj.LabelSelector).String()
		listOptions := metav1.ListOptions{
			LabelSelector: labelSelector,
		}

		// Try to find the list of resources by GroupVersionResource.
		err := util.RetryWithExponentialBackOff(func() (done bool, err error) {
			itemList, err = DynamicClient.Resource(obj.gvr).List(context.TODO(), listOptions)
			if err != nil {
				log.Errorf("Error found listing %s labeled with %s: %s", obj.gvr.Resource, labelSelector, err)
				return false, nil
			}
			return true, nil
		}, 1*time.Second, 3, 0, ex.MaxWaitTimeout)
		if err != nil {
			continue
		}
		log.Infof("Found %d %s with selector %s; patching them", len(itemList.Items), obj.gvr.Resource, labelSelector)

		for _, item := range itemList.Items {
			wg.Add(1)
			go ex.kubeOpHandler(virtClient, obj, item, &wg)
		}
	}
	wg.Wait()
	waitRateLimiter := rate.NewLimiter(rate.Limit(restConfig.QPS), restConfig.Burst)
	ex.waitForObjects("", waitRateLimiter)
}

func (ex *Executor) kubeOpHandler(virtClient kubecli.KubevirtClient, obj object, item unstructured.Unstructured, wg *sync.WaitGroup) {
	defer wg.Done()

	var err error
	switch obj.KubeVirtOp {
	case config.KubeVirtOpStart:
		err = virtClient.VirtualMachine(item.GetNamespace()).Start(context.Background(), item.GetName(), &kubevirtV1.StartOptions{Paused: false /*startPaused*/, DryRun: nil})
	case config.KubeVirtOpStop:
		stopOpts := &kubevirtV1.StopOptions{DryRun: nil}
		// if forceRestart {
		// 	stopOpts.GracePeriod = &gracePeriod
		// 	errorFmt = "error force stopping VirtualMachine: %v"
		// }
		err = virtClient.VirtualMachine(item.GetNamespace()).Stop(context.Background(), item.GetName(), stopOpts)
	case config.KubeVirtOpRestart:
		restartOpts := &kubevirtV1.RestartOptions{DryRun: nil}
		// if forceRestart {
		// 	restartOpts.GracePeriodSeconds = &gracePeriod
		// 	errorFmt = "error force restarting VirtualMachine: %v"
		// }
		err = virtClient.VirtualMachine(item.GetNamespace()).Restart(context.Background(), item.GetName(), restartOpts)
	case config.KubeVirtOpPause:
		err = virtClient.VirtualMachineInstance(item.GetNamespace()).Pause(context.Background(), item.GetName(), &kubevirtV1.PauseOptions{DryRun: nil})
	case config.KubeVirtOpUnpause:
		err = virtClient.VirtualMachineInstance(item.GetNamespace()).Unpause(context.Background(), item.GetName(), &kubevirtV1.UnpauseOptions{DryRun: nil})
	}

	if err != nil {
		log.Errorf("Failed to execute op [%s] on the VM [%s]: %v", obj.KubeVirtOp, item.GetName(), err)
	} else {
		log.Debugf("Successfully executed op [%s] on the VM [%s]", obj.KubeVirtOp, item.GetName())
	}

}
