// Copyright 2020 The Kube-burner Authors.
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
	"encoding/json"
	"time"

	"github.com/itchyny/gojq"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/kube-burner/kube-burner/pkg/burner/types"
)

func (ex *Executor) waitForObjects(ns string, limiter *rate.Limiter) {
	for _, obj := range ex.objects {
		waitNs := ns
		if !obj.Wait {
			continue
		}
		// When the object has defined its own namespace, we use it
		// TODO objects with a fixed namespace don't need to be waited on a per iteration basis
		if obj.namespace != "" {
			waitNs = obj.namespace
		}
		if obj.WaitOptions.ForCondition != "" {
			ex.verifyCondition(waitNs, obj, limiter)
		} else {
			kind := obj.kind
			if obj.WaitOptions.Kind != "" {
				kind = obj.WaitOptions.Kind
				waitNs = ""
			}
			switch kind {
			case Deployment:
				ex.waitForDeployments(waitNs, obj, limiter)
			case ReplicaSet:
				ex.waitForRS(waitNs, obj, limiter)
			case ReplicationController:
				ex.waitForRC(waitNs, obj, limiter)
			case StatefulSet:
				ex.waitForStatefulSet(waitNs, obj, limiter)
			case DaemonSet:
				ex.waitForDS(waitNs, obj, limiter)
			case Pod:
				ex.waitForPod(waitNs, obj, limiter)
			case Build, BuildConfig:
				ex.waitForBuild(waitNs, obj, limiter)
			case VirtualMachine:
				ex.waitForVM(waitNs, obj, limiter)
			case VirtualMachineInstance:
				ex.waitForVMI(waitNs, obj, limiter)
			case VirtualMachineInstanceReplicaSet:
				ex.waitForVMIRS(waitNs, obj, limiter)
			case Job:
				ex.waitForJob(waitNs, obj, limiter)
			case PersistentVolumeClaim:
				ex.waitForPVC(waitNs, obj, limiter)
			}
		}
	}
	log.Infof("Actions in namespace %v completed", ns)
}

func (ex *Executor) waitForDeployments(ns string, obj object, limiter *rate.Limiter) {
	// TODO handle errors such as timeouts
	wait.PollUntilContextTimeout(context.TODO(), time.Second, ex.MaxWaitTimeout, true, func(ctx context.Context) (done bool, err error) {
		limiter.Wait(context.TODO())
		deps, err := ex.clientSet.AppsV1().Deployments(ns).List(context.TODO(), metav1.ListOptions{
			LabelSelector: labels.Set(obj.WaitOptions.LabelSelector).String(),
		})
		if err != nil {
			return false, err
		}
		for _, dep := range deps.Items {
			if *dep.Spec.Replicas != dep.Status.ReadyReplicas {
				log.Debugf("Waiting for replicas from deployments in ns %s to be ready", ns)
				return false, nil
			}
		}
		return true, nil
	})
}

func (ex *Executor) waitForRS(ns string, obj object, limiter *rate.Limiter) {
	wait.PollUntilContextTimeout(context.TODO(), time.Second, ex.MaxWaitTimeout, true, func(ctx context.Context) (done bool, err error) {
		limiter.Wait(context.TODO())
		rss, err := ex.clientSet.AppsV1().ReplicaSets(ns).List(context.TODO(), metav1.ListOptions{
			LabelSelector: labels.Set(obj.WaitOptions.LabelSelector).String(),
		})
		if err != nil {
			return false, err
		}
		for _, rs := range rss.Items {
			if *rs.Spec.Replicas != rs.Status.ReadyReplicas {
				log.Debugf("Waiting for replicas from replicaSets in ns %s to be ready", ns)
				return false, nil
			}
		}
		return true, nil
	})
}

func (ex *Executor) waitForStatefulSet(ns string, obj object, limiter *rate.Limiter) {
	wait.PollUntilContextTimeout(context.TODO(), time.Second, ex.MaxWaitTimeout, true, func(ctx context.Context) (done bool, err error) {
		limiter.Wait(context.TODO())
		stss, err := ex.clientSet.AppsV1().StatefulSets(ns).List(context.TODO(), metav1.ListOptions{
			LabelSelector: labels.Set(obj.WaitOptions.LabelSelector).String(),
		})
		if err != nil {
			return false, err
		}
		for _, sts := range stss.Items {
			if *sts.Spec.Replicas != sts.Status.ReadyReplicas {
				log.Debugf("Waiting for replicas from statefulSets in ns %s to be ready", ns)
				return false, nil
			}
		}
		return true, nil
	})
}

func (ex *Executor) waitForPVC(ns string, obj object, limiter *rate.Limiter) {
	wait.PollUntilContextTimeout(context.TODO(), time.Second, ex.MaxWaitTimeout, true, func(ctx context.Context) (done bool, err error) {
		limiter.Wait(context.TODO())
		pvcs, err := ex.clientSet.CoreV1().PersistentVolumeClaims(ns).List(context.TODO(), metav1.ListOptions{
			LabelSelector: labels.Set(obj.WaitOptions.LabelSelector).String(),
		})
		if err != nil {
			return false, err
		}
		for _, pvc := range pvcs.Items {
			if pvc.Status.Phase != corev1.ClaimBound {
				log.Debugf("Waiting for pvcs in ns %s to be Bound", ns)
				return false, nil
			}
		}
		return true, nil
	})
}

func (ex *Executor) waitForRC(ns string, obj object, limiter *rate.Limiter) {
	wait.PollUntilContextTimeout(context.TODO(), time.Second, ex.MaxWaitTimeout, true, func(ctx context.Context) (done bool, err error) {
		limiter.Wait(context.TODO())
		rcs, err := ex.clientSet.CoreV1().ReplicationControllers(ns).List(context.TODO(), metav1.ListOptions{
			LabelSelector: labels.Set(obj.WaitOptions.LabelSelector).String(),
		})
		if err != nil {
			return false, err
		}
		for _, rc := range rcs.Items {
			if *rc.Spec.Replicas != rc.Status.ReadyReplicas {
				log.Debugf("Waiting for replicas from replicationControllers in ns %s to be ready", ns)
				return false, nil
			}
		}
		return true, nil
	})
}

func (ex *Executor) waitForDS(ns string, obj object, limiter *rate.Limiter) {
	wait.PollUntilContextTimeout(context.TODO(), time.Second, ex.MaxWaitTimeout, true, func(ctx context.Context) (done bool, err error) {
		limiter.Wait(context.TODO())
		dss, err := ex.clientSet.AppsV1().DaemonSets(ns).List(context.TODO(), metav1.ListOptions{
			LabelSelector: labels.Set(obj.WaitOptions.LabelSelector).String(),
		})
		if err != nil {
			return false, err
		}
		for _, ds := range dss.Items {
			if ds.Status.DesiredNumberScheduled != ds.Status.NumberReady {
				log.Debugf("Waiting for replicas from daemonsets in ns %s to be ready", ns)
				return false, nil
			}
		}
		return true, nil
	})
}

func (ex *Executor) waitForPod(ns string, obj object, limiter *rate.Limiter) {
	wait.PollUntilContextTimeout(context.TODO(), time.Second, ex.MaxWaitTimeout, true, func(ctx context.Context) (done bool, err error) {
		// We need to paginate these requests to ensure we don't miss any pods
		listOptions := metav1.ListOptions{
			Limit:         1000,
			LabelSelector: labels.Set(obj.WaitOptions.LabelSelector).String(),
		}
		for {
			limiter.Wait(context.TODO())
			pods, err := ex.clientSet.CoreV1().Pods(ns).List(context.TODO(), listOptions)
			listOptions.Continue = pods.GetContinue()
			for _, pod := range pods.Items {
				if pod.Status.Phase != corev1.PodRunning {
					return false, nil
				}
				for _, c := range pod.Status.Conditions {
					if c.Type == corev1.PodReady && c.Status == corev1.ConditionFalse {
						return false, nil
					}
				}
			}
			if err != nil {
				return false, err
			}
			if listOptions.Continue == "" {
				break
			}
		}
		return true, nil
	})
}

func (ex *Executor) waitForBuild(ns string, obj object, limiter *rate.Limiter) {
	buildStatus := []string{"New", "Pending", "Running"}
	var build types.UnstructuredContent
	gvr := schema.GroupVersionResource{
		Group:    types.OpenShiftBuildGroup,
		Version:  types.OpenShiftBuildAPIVersion,
		Resource: types.OpenShiftBuildResource,
	}
	wait.PollUntilContextTimeout(context.TODO(), time.Second, ex.MaxWaitTimeout, true, func(ctx context.Context) (done bool, err error) {
		limiter.Wait(context.TODO())
		builds, err := ex.dynamicClient.Resource(gvr).Namespace(ns).List(context.TODO(), metav1.ListOptions{
			LabelSelector: labels.Set(obj.WaitOptions.LabelSelector).String(),
		})
		if err != nil {
			return false, err
		}
		if len(builds.Items) < obj.Replicas {
			log.Debugf("Waiting for Builds in ns %s to be completed", ns)
			return false, err
		}
		for _, b := range builds.Items {
			jsonBuild, err := b.MarshalJSON()
			if err != nil {
				log.Errorf("Error decoding Build object: %s", err)
			}
			_ = json.Unmarshal(jsonBuild, &build)
			for _, bs := range buildStatus {
				if build.Status.Phase == "" || build.Status.Phase == bs {
					log.Debugf("Waiting for Builds in ns %s to be completed", ns)
					return false, err
				}
			}
		}
		return true, nil
	})
}

func (ex *Executor) waitForJob(ns string, obj object, limiter *rate.Limiter) {
	if obj.WaitOptions.ForCondition == "" {
		obj.WaitOptions.ForCondition = "Complete"
	}
	ex.verifyCondition(ns, obj, limiter)
}

func (ex *Executor) verifyCondition(ns string, obj object, limiter *rate.Limiter) {
	var uObj types.UnstructuredContent
	wait.PollUntilContextTimeout(context.TODO(), time.Second, ex.MaxWaitTimeout, true, func(ctx context.Context) (done bool, err error) {
		var objs *unstructured.UnstructuredList
		limiter.Wait(context.TODO())
		if obj.Namespaced {
			objs, err = ex.dynamicClient.Resource(obj.gvr).Namespace(ns).List(context.TODO(), metav1.ListOptions{
				LabelSelector: labels.Set(obj.WaitOptions.LabelSelector).String(),
			})
		} else {
			objs, err = ex.dynamicClient.Resource(obj.gvr).List(context.TODO(), metav1.ListOptions{
				LabelSelector: labels.Set(obj.WaitOptions.LabelSelector).String(),
			})
		}
		if err != nil {
			return false, err
		}
	VERIFY:
		for _, item := range objs.Items {
			if obj.WaitOptions.CustomStatusPath != "" {
				status, found, err := unstructured.NestedMap(item.Object, "status")
				if err != nil || !found {
					log.Errorf("Error extracting or finding status in object %s/%s: %v", item.GetKind(), item.GetName(), err)
					return false, err
				}
				if len(status) != 0 {
					// Compile and execute the jq query
					query, err := gojq.Parse(obj.WaitOptions.CustomStatusPath)
					if err != nil {
						log.Errorf("Error parsing jq path: %s", obj.WaitOptions.CustomStatusPath)
						return false, err
					}
					iter := query.Run(status)
					for {
						v, ok := iter.Next()
						if !ok {
							break
						}
						if err, ok := v.(error); ok {
							log.Errorf("Error evaluating jq path: %s", err)
							return false, err
						}
						if v == obj.WaitOptions.ForCondition {
							continue VERIFY
						}
					}
				}
			} else {
				jsonBuild, err := item.MarshalJSON()
				if err != nil {
					log.Errorf("Error decoding object: %s", err)
					return false, err
				}
				_ = json.Unmarshal(jsonBuild, &uObj)
				for _, c := range uObj.Status.Conditions {
					if c.Status == "True" && c.Type == obj.WaitOptions.ForCondition {
						continue VERIFY
					}
				}
			}
			if obj.Namespaced {
				log.Debugf("Waiting for %s in ns %s to be ready", obj.gvr.Resource, ns)
			} else {
				log.Debugf("Waiting for %s to be ready", obj.gvr.Resource)
			}
			return false, err
		}
		return true, nil
	})
}

func (ex *Executor) waitForVM(ns string, obj object, limiter *rate.Limiter) {
	if obj.WaitOptions.ForCondition == "" {
		obj.WaitOptions.ForCondition = "Ready"
	}
	ex.verifyCondition(ns, obj, limiter)
}

func (ex *Executor) waitForVMI(ns string, obj object, limiter *rate.Limiter) {
	if obj.WaitOptions.ForCondition == "" {
		obj.WaitOptions.ForCondition = "Ready"
	}
	ex.verifyCondition(ns, obj, limiter)
}

func (ex *Executor) waitForVMIRS(ns string, obj object, limiter *rate.Limiter) {
	var rs types.UnstructuredContent
	vmiGVRRS := schema.GroupVersionResource{
		Group:    types.KubevirtGroup,
		Version:  types.KubevirtAPIVersion,
		Resource: types.VirtualMachineInstanceReplicaSetResource,
	}
	wait.PollUntilContextTimeout(context.TODO(), time.Second, ex.MaxWaitTimeout, true, func(ctx context.Context) (done bool, err error) {
		limiter.Wait(context.TODO())
		objs, err := ex.dynamicClient.Resource(vmiGVRRS).Namespace(ns).List(context.TODO(), metav1.ListOptions{
			LabelSelector: labels.Set(obj.WaitOptions.LabelSelector).String(),
		})
		if err != nil {
			log.Debugf("VMIRS error %v", err)
			return false, err
		}
		for _, item := range objs.Items {
			jsonBuild, err := item.MarshalJSON()
			if err != nil {
				log.Errorf("Error decoding VMIRS object: %s", err)
				return false, err
			}
			_ = json.Unmarshal(jsonBuild, &rs)
			if rs.Spec.Replicas != rs.Status.ReadyReplicas {
				log.Debugf("Waiting for replicas from VMIRS in ns %s to be running", ns)
				return false, nil
			}
		}
		return true, nil
	})
}
