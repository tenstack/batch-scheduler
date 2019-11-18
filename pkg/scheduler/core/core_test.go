/*
 * Copyright 2020 THL A29 Limited, a Tencent company.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package core

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

func Test_singleNodeResurouce(t *testing.T) {
	pod := corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("1"),
							corev1.ResourceName("alpha.kubernetes.io/nvidia-gpu"): resource.MustParse("1"),
							corev1.ResourceName("tencent.cr/tencentip"):           resource.MustParse("1"),
						},
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("1"),
							corev1.ResourceName("alpha.kubernetes.io/nvidia-gpu"): resource.MustParse("1"),

							corev1.ResourceName("tencent.cr/tencentip"): resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	node := corev1.Node{
		Spec: corev1.NodeSpec{},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("10"),
				corev1.ResourceName("alpha.kubernetes.io/nvidia-gpu"): resource.MustParse("10"),

				corev1.ResourcePods:                         resource.MustParse("100"),
				corev1.ResourceName("tencent.cr/tencentip"): resource.MustParse("20"),
			},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("10"),
				corev1.ResourceName("alpha.kubernetes.io/nvidia-gpu"): resource.MustParse("10"),

				corev1.ResourcePods:                         resource.MustParse("100"),
				corev1.ResourceName("tencent.cr/tencentip"): resource.MustParse("20"),
			},
		},
	}

	nodeIf := nodeinfo.NewNodeInfo()
	nodeIf.SetNode(&node)
	nodeIf.AddPod(&pod)
	podCopy := pod.DeepCopy()
	podCopy.Spec.Containers[0].Resources.Requests[corev1.ResourceName("alpha.kubernetes.io/nvidia-gpu")] = resource.MustParse("101")
	podCopy.Spec.Containers[0].Resources.Limits[corev1.ResourceName("alpha.kubernetes.io/nvidia-gpu")] = resource.MustParse("101")
	podCopy1 := pod.DeepCopy()

	podCopy1.Spec.Containers[0].Resources.Requests[corev1.ResourceName("tencent.cr/tencentip")] = resource.MustParse(
		"101")
	podCopy1.Spec.Containers[0].Resources.Limits[corev1.ResourceName("tencent.cr/tencentip")] = resource.MustParse(
		"101")
	cases := []struct {
		name   string
		node   *nodeinfo.NodeInfo
		pod    *corev1.Pod
		desire bool
	}{
		{
			node:   nodeIf,
			pod:    &pod,
			desire: true,
		},
		{
			node:   nodeIf,
			pod:    podCopy,
			desire: false,
		},
		{
			node:   nodeIf,
			pod:    podCopy1,
			desire: false,
		},
	}

	for _, c := range cases {
		t.Logf("%+v", c.node.AllocatableResource())
		t.Logf("%+v", c.node.RequestedResource())
		re := singleNodeResource(c.node, c.pod, 1)
		req := getPodResourceRequire(c.pod)
		if compareResourceAndRequire(re, req) != c.desire {
			t.Error("not desire")
		}
		t.Logf("----\n %+v", req)
	}
}
