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

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	kubevirtV1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"

	"github.com/kube-burner/kube-burner/pkg/config"
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
	var err error
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

	ex.kubeVirtClient, err = kubecli.GetKubevirtClientFromRESTConfig(restConfig)
	if err != nil {
		log.Fatalf("Failed to get kubevirt client - %v", err)
	}

	return ex
}

func (ex *Executor) RunKubeVirtJob(restConfig *rest.Config) {
	if ex.kubeVirtClient == nil {
		log.Fatal("Run job was called without calling setup")
	}
	ex.runJob(ex.kubeOpHandler)
}

func (ex *Executor) kubeOpHandler(obj object, item unstructured.Unstructured, iteration int, wg *sync.WaitGroup) {
	defer wg.Done()

	var err error
	switch obj.KubeVirtOp {
	case config.KubeVirtOpStart:
		err = ex.kubeVirtClient.VirtualMachine(item.GetNamespace()).Start(context.Background(), item.GetName(), &kubevirtV1.StartOptions{Paused: false /*startPaused*/, DryRun: nil})
	case config.KubeVirtOpStop:
		stopOpts := &kubevirtV1.StopOptions{DryRun: nil}
		// if forceRestart {
		// 	stopOpts.GracePeriod = &gracePeriod
		// 	errorFmt = "error force stopping VirtualMachine: %v"
		// }
		err = ex.kubeVirtClient.VirtualMachine(item.GetNamespace()).Stop(context.Background(), item.GetName(), stopOpts)
	case config.KubeVirtOpRestart:
		restartOpts := &kubevirtV1.RestartOptions{DryRun: nil}
		// if forceRestart {
		// 	restartOpts.GracePeriodSeconds = &gracePeriod
		// 	errorFmt = "error force restarting VirtualMachine: %v"
		// }
		err = ex.kubeVirtClient.VirtualMachine(item.GetNamespace()).Restart(context.Background(), item.GetName(), restartOpts)
	case config.KubeVirtOpPause:
		err = ex.kubeVirtClient.VirtualMachineInstance(item.GetNamespace()).Pause(context.Background(), item.GetName(), &kubevirtV1.PauseOptions{DryRun: nil})
	case config.KubeVirtOpUnpause:
		err = ex.kubeVirtClient.VirtualMachineInstance(item.GetNamespace()).Unpause(context.Background(), item.GetName(), &kubevirtV1.UnpauseOptions{DryRun: nil})
	}

	if err != nil {
		log.Errorf("Failed to execute op [%s] on the VM [%s]: %v", obj.KubeVirtOp, item.GetName(), err)
	} else {
		log.Debugf("Successfully executed op [%s] on the VM [%s]", obj.KubeVirtOp, item.GetName())
	}
}
