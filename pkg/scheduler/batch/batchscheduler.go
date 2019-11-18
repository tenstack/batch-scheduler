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

package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"

	v1 "github.com/tenstack/batch-scheduler/pkg/apis/podgroup/v1"
	pgclientset "github.com/tenstack/batch-scheduler/pkg/generated/clientset/versioned"
	pgformers "github.com/tenstack/batch-scheduler/pkg/generated/informers/externalversions"
	"github.com/tenstack/batch-scheduler/pkg/scheduler/cache"
	"github.com/tenstack/batch-scheduler/pkg/scheduler/controller"
	"github.com/tenstack/batch-scheduler/pkg/scheduler/core"
	"github.com/tenstack/batch-scheduler/pkg/util"
)

// batchScheduler support schedule batch pods
type batchScheduler interface {
	// UpdateBatchCache update the pod cache which should be scheduled as a group
	UpdateBatchCache()
	// StartBatchSchedule receives the group name of pod to start scheduling
	StartBatchSchedule(string)
}

type batchSchedulingPlugin struct {
	frameworkHandler framework.FrameworkHandle
	operation        *core.ScheduleOperation
	startChan        chan string
	maxScheduleTime  *time.Duration
	client           pgclientset.Interface
	extension        *batchSchedulingPluginExtension
	sync.RWMutex
}

// Configuration defines the config for batch scheduler
type Configuration struct {
	KubeMaster      string `json:"kube_master,omitempty"`
	KubeConfig      string `json:"kube_config,omitempty"`
	MaxScheduleTime int64  `json:"max_schedule_time,omitempty"`
}

var _ batchScheduler = &batchSchedulingPlugin{}
var _ framework.PreFilterPlugin = &batchSchedulingPlugin{}
var _ framework.FilterPlugin = &batchSchedulingPlugin{}
var _ framework.PermitPlugin = &batchSchedulingPlugin{}
var _ framework.PostBindPlugin = &batchSchedulingPlugin{}
var _ framework.QueueSortPlugin = &batchSchedulingPlugin{}

// Name is the name of the plug used in Registry and configurations.
const (
	Name            = "batch-scheduler"
	retryAllowLimit = 50
	getWaitPodLimit = 3
)

// Name returns name of the plugin. It is used in logs, etc.
func (bs *batchSchedulingPlugin) Name() string {
	return Name
}

type batchSchedulingPluginExtension struct {
	operation *core.ScheduleOperation
}

// PreFilter is called at the beginning of the scheduling cycle. All PreFilter
// plugins must return success or the pod will be rejected.
func (bs *batchSchedulingPlugin) PreFilter(ctx context.Context, state *framework.CycleState,
	p *corev1.Pod) *framework.Status {
	if err := bs.operation.PreFilter(p); err != nil {
		return framework.NewStatus(framework.Unschedulable, err.Error())
	}
	return framework.NewStatus(framework.Success, "")
}

// PreFilterExtensions returns a PreFilterExtensions interface if the plugin implements one,
// or nil if it does not. A Pre-filter plugin can provide extensions to incrementally
// modify its pre-processed info. The framework guarantees that the extensions
// AddPod/RemovePod will only be called after PreFilter, possibly on a cloned
// CycleState, and may call those functions more than once before calling
// Filter again on a specific node.
func (bs *batchSchedulingPlugin) PreFilterExtensions() framework.PreFilterExtensions {
	return bs.extension
}

// AddPod is called by the framework while trying to evaluate the impact
// of adding podToAdd to the node while scheduling podToSchedule.
func (bs *batchSchedulingPluginExtension) AddPod(ctx context.Context, state *framework.CycleState,
	podToSchedule *corev1.Pod, podToAdd *corev1.Pod, nodeInfo *schedulernodeinfo.NodeInfo) *framework.Status {
	if err := bs.operation.PreemptAddPod(podToAdd, nodeInfo.Node().Name); err != nil {
		return framework.NewStatus(framework.Unschedulable, err.Error())
	}
	return framework.NewStatus(framework.Success, "")
}

// RemovePod is called by the framework while trying to evaluate the impact
// of removing podToRemove from the node while scheduling podToSchedule.
func (bs *batchSchedulingPluginExtension) RemovePod(ctx context.Context, state *framework.CycleState,
	podToSchedule *corev1.Pod, podToRemove *corev1.Pod, nodeInfo *schedulernodeinfo.NodeInfo) *framework.Status {
	//klog.V(5).Infof("Batch scheduler try to remove pod %s/%s add pod %s/%s",
	//	podToRemove.Namespace, podToRemove.Name, podToSchedule.Namespace, podToSchedule.Name)
	if err := bs.operation.PreemptRemovePod(podToSchedule, podToRemove); err != nil {
		if klog.V(5) {
			klog.Error(err)
		}
		return framework.NewStatus(framework.Unschedulable, err.Error())
	}
	klog.V(5).Infof("batchSchedulingPluginExtension pass remove pod %v", podToRemove.Name)
	return framework.NewStatus(framework.Success, "")
}

// Filter is called by the scheduling framework.
// All FilterPlugins should return "Success" to declare that
// the given node fits the pod. If Filter doesn't return "Success",
// please refer scheduler/algorithm/predicates/error.go
// to set error message.
func (bs *batchSchedulingPlugin) Filter(ctx context.Context, state *framework.CycleState, p *corev1.Pod,
	node *schedulernodeinfo.NodeInfo) *framework.Status {
	if err := bs.operation.Filter(p, node.Node().Name); err != nil {
		return framework.NewStatus(framework.Unschedulable, err.Error())
	}
	return framework.NewStatus(framework.Success, "")
}

// Permit is called before binding a pod (and before prebind plugins). Permit
// plugins are used to prevent or delay the binding of a Pod. A permit plugin
// must return success or wait with timeout duration, or the pod will be rejected.
// The pod will also be rejected if the wait timeout or the pod is rejected while
// waiting. Note that if the plugin returns "wait", the framework will wait only
// after running the remaining plugins given that no other plugin rejects the pod.
func (bs *batchSchedulingPlugin) Permit(ctx context.Context, state *framework.CycleState, p *corev1.Pod, nodeName string) (*framework.Status, time.Duration) {
	var pgs *cache.PodGroupMatchStatus
	fullName := ""
	ready, pgName, err := bs.operation.Permit(p, nodeName)
	if pgName != "" {
		fullName = fmt.Sprintf("%v/%v", p.Namespace, pgName)
		pgObj := bs.operation.PodGroupStatusCache().Get(fullName)
		if pgObj != nil {
			pgs = pgObj.(*cache.PodGroupMatchStatus)
		}
	}
	waitTime := util.DefaultWaitTime
	if pgs != nil {
		waitTime = util.GetWaitTimeDuration(pgs.PodGroup, bs.maxScheduleTime)
	}
	// add 1 second to keep the ttl cache would
	// expired before the the waiting deadline
	waitTime = waitTime + 1*time.Second
	if err != nil {
		if err == util.ErrorWaiting {
			klog.Infof("Pod: %v/%v is waiting to be scheduled to node: %v", p.Namespace, p.Name, nodeName)
			return framework.NewStatus(framework.Wait, ""), waitTime
		}
		// For pod not belongs to any groups
		if err == util.ErrorNotMatched {
			return framework.NewStatus(framework.Success, ""), 0

		}
		klog.Infof("bs.operation.Permit error %v", err)
		return framework.NewStatus(framework.Unschedulable, err.Error()), util.DefaultWaitTime
	}
	klog.V(5).Infof("Pod requires pgName %v", pgName)
	if ready {
		go bs.sendStartScheduleSignal(fullName)
	}

	return framework.NewStatus(framework.Wait, ""), waitTime
}

// PostBind is called after a pod is successfully bound. These plugins are
// informational. A common application of this extension point is for cleaning
// up. If a plugin needs to clean-up its state after a pod is scheduled and
// bound, Postbind is the extension point that it should register.
func (bs *batchSchedulingPlugin) PostBind(ctx context.Context, state *framework.CycleState, p *corev1.Pod, nodeName string) {
	klog.V(5).Infof("PostBind pod: %v/%v", p.Namespace, p.Name)
	bs.operation.PostBind(p, nodeName)
}

// Less are used to sort pods in the scheduling queue.
func (bs *batchSchedulingPlugin) Less(pi1 *framework.PodInfo, pi2 *framework.PodInfo) bool {
	return bs.operation.Compare(pi1, pi2)
}

// UpdateLocalCache upadte pgstatus cache
func (bs *batchSchedulingPlugin) UpdateBatchCache() {
	// update pods cache
	bs.frameworkHandler.IterateOverWaitingPods(func(waitingPod framework.WaitingPod) {
		klog.V(5).Info("Start IterateOverWaitingPods")
		pod := waitingPod.GetPod()
		pgName, satisfied := util.VerifyPodLabelSatisfied(pod)
		klog.V(5).Infof("Start walking through pod %v/%v uid: %v", pod.Namespace, pod.Name, pod.UID)
		if satisfied && len(pgName) != 0 {
			fullName := fmt.Sprintf("%v/%v", pod.Namespace, pgName)
			pgsObj := bs.operation.PodGroupStatusCache().Get(fullName)
			if pgsObj == nil {
				return
			}
			pgs := pgsObj.(*cache.PodGroupMatchStatus)
			klog.V(5).Infof("Wanted cache pod %v/%v", pod.Namespace, pod.Name)
			oldUID, found := pgs.PodNameUIDs.Get(fmt.Sprintf("%v/%v", pod.Namespace, pod.Name))
			// pod has been scheduled ever
			if found {
				if oldUID.(string) != string(pod.UID) {
					// delete the expired one
					pgs.MatchedPodNodes.Delete(oldUID.(string))
					pgs.PodNameUIDs.Delete(fmt.Sprintf("%v/%v", pod.Namespace, pod.Name))
					klog.V(3).Infof("Delete old cache data %v", oldUID)
				}
			} else {
				// newly add one
				klog.V(3).Infof("Add new cache data %v", pod.UID)
			}
			klog.V(6).Infof("Current pod group state %+v", pgs.PodNameUIDs.Items())
			bs.operation.PodGroupStatusCache().Set(util.GetPodGroupFullName(pgs.PodGroup), pgs)
		}
	})
}

// StartBatchSchedule receives the group name of pod to start scheduling
func (bs *batchSchedulingPlugin) StartBatchSchedule(fullName string) {

	pgCache := bs.operation.PodGroupStatusCache().(*cache.PGStatusCache)
	pgs := pgCache.Get(fullName).(*cache.PodGroupMatchStatus)
	if pgs.PodGroup.Status.Phase != v1.PodGroupPreScheduling && pgs.PodGroup.Status.Phase != v1.
		PodGroupScheduling {
		return
	}

	// record time to avoid abnormal exit when bind
	if pgs.PodGroup.Status.Scheduled >= pgs.PodGroup.Spec.MinMember {
		pg, err := bs.client.BatchV1().PodGroups(pgs.PodGroup.Namespace).Get(pgs.PodGroup.Name, v12.GetOptions{})
		defer func() {
			if err != nil {
				bs.startChan <- fullName
			}
		}()
		if err != nil {
			klog.Error(err)
			return
		}
		pgCopy := pg.DeepCopy()
		pgCopy.Status.ScheduleStartTime = v12.Now()

		var patch []byte
		patch, err = util.CreateMergePatch(pg, pgCopy)
		if err != nil {
			return
		}

		pg, err = bs.client.BatchV1().PodGroups(pg.Namespace).Patch(pg.Name, types.MergePatchType, patch)
		if err != nil {
			return
		}
	}

	// Start batch scheduling
	klog.V(4).Infof("Start batch scheduling %v", fullName)
	pendingPods := bs.operation.GetPodNodePairs(fullName)
	if pendingPods == nil {
		klog.V(4).Infof("Can not found pending pods for %v", fullName)
		return
	}
	pendingPodNameIDs := bs.operation.GetPodNameUIDs(fullName)
	if pendingPodNameIDs == nil {
		klog.V(4).Infof("Can not found pending IDs for %v", fullName)
		return
	}
	pendingPodsMap := pendingPods.Items()
	if uint32(len(pendingPodsMap)) < pgs.PodGroup.Spec.MinMember-pgs.PodGroup.Status.Scheduled {
		return
	}

	klog.V(5).Infof("Current pod group: %v state %+v, count: %d", fullName, pendingPodNameIDs,
		len(pendingPodsMap))

	for uid, pair := range pendingPodsMap {
		// double check
		var waitingPod framework.WaitingPod
		for i := 0; i < getWaitPodLimit; i++ {
			waitingPod = bs.frameworkHandler.GetWaitingPod(types.UID(uid))
			if waitingPod == nil {
				if i == 2 {
					// to avoid sig send, but scheduler cache have not been flushed
					klog.V(4).Infof("Remove pod uid %v, pair %v", uid, pair.Object)
					pendingPods.Delete(uid)
					pnPair := pair.Object.(*cache.PodNodePair)
					pendingPodNameIDs.Delete(pnPair.PodName)
					return
				}
				time.Sleep(10 * time.Millisecond)
				continue

			}
		}
		for i := 0; i < retryAllowLimit; i++ {
			if success := waitingPod.Allow(Name); success {
				klog.V(5).Infof("Approved pod %+v", pair.Object)
				pendingPods.Delete(uid)
				pendingPodNameIDs.Delete(uid)
				break
			}
			time.Sleep(50 * time.Millisecond)
			if i == 50 {
				klog.Warningf("Approved pod %+v failed", pair.Object)
				return
			}

		}
	}
}

// rejectPod rejects pod in cache
func (bs *batchSchedulingPlugin) rejectPod(uid types.UID) {
	waitingPod := bs.frameworkHandler.GetWaitingPod(uid)
	if success := waitingPod.Reject("Group failed"); success {
		return
	}
	klog.Warningf("Rejected pod %+v failed", uid)

}

// ReconcileStatus reconcile pod cache states and decide when to schedule
func (bs *batchSchedulingPlugin) ReconcileStatus(stopChan <-chan struct{}) {
	for {
		select {
		case pgName := <-bs.startChan:
			bs.UpdateBatchCache()
			bs.StartBatchSchedule(pgName)
		case <-stopChan:
			klog.Info("Reconcile exit")
			return
		}
	}
}

// sendStartScheduleSignal send stat scheduling signal to scheduler
func (bs *batchSchedulingPlugin) sendStartScheduleSignal(pgName string) {
	bs.startChan <- pgName
	klog.Info("Send StartScheduleSignal success")
}

// New initializes a new plugin and returns it.
func New(configuration *runtime.Unknown, f framework.FrameworkHandle) (framework.Plugin, error) {
	var config Configuration
	// TODO: decode it in a better way
	if err := json.Unmarshal(configuration.Raw, &config); err != nil {
		klog.Errorf("Failed to decode %+v: %v", configuration.Raw, err)
		return nil, fmt.Errorf("failed to decode configuration: %v", err)
	}

	klog.V(4).Infof("Plugin %s's config: master(%s), kube-config(%s)", Name, config.KubeMaster, config.KubeConfig)
	// Init client and Informer
	c, err := clientcmd.BuildConfigFromFlags(config.KubeMaster, config.KubeConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to init rest.Config: %v", err)
	}
	c.QPS = 10
	c.Burst = 20

	pgClient := pgclientset.NewForConfigOrDie(c)
	scheduleInformer := pgformers.NewSharedInformerFactory(pgClient, 0)
	pgInformer := scheduleInformer.Batch().V1().PodGroups()

	kubeClient := f.ClientSet()

	pgCache := cache.NewPGStatusCache()
	startChan := make(chan string)

	extAPIClient := apiextensionsclient.NewForConfigOrDie(c)

	//resourceCache := cache.NewNodeResourceCache()
	scheduleTimeDuration := time.Duration(config.MaxScheduleTime) * time.Minute
	pgOperation := core.NewScheduleOperation(pgClient, pgCache, f, &scheduleTimeDuration, pgInformer)
	plugin := &batchSchedulingPlugin{
		frameworkHandler: f,
		operation:        pgOperation,
		startChan:        startChan,
		maxScheduleTime:  &scheduleTimeDuration,
		client:           pgClient,
	}

	crd := &apiextensionsv1beta1.CustomResourceDefinition{
		ObjectMeta: v12.ObjectMeta{
			Name: "podgroups.batch.scheduler.tencent.com",
		},
		Spec: apiextensionsv1beta1.CustomResourceDefinitionSpec{
			Group:   "batch.scheduler.tencent.com",
			Version: "v1",
			Scope:   apiextensionsv1beta1.NamespaceScoped,
			Names: apiextensionsv1beta1.CustomResourceDefinitionNames{
				Kind:       "PodGroup",
				Plural:     "podgroups",
				ShortNames: []string{"pg", "pgs"},
			},
		},
	}

	_, err = extAPIClient.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
	if err != nil && !apierrs.IsAlreadyExists(err) {
		klog.Errorf("Failed to create crd %v", err.Error())
		return nil, err
	}

	ctx := context.TODO()
	go plugin.ReconcileStatus(ctx.Done())

	rateLimiter := workqueue.NewItemExponentialFailureRateLimiter(time.Second, 10*time.Second)
	controller := controller.NewPodGroupController(kubeClient, pgInformer, rateLimiter, pgClient, pgCache,
		plugin.rejectPod, pgOperation.AddToDenyCache)
	scheduleInformer.Start(ctx.Done())

	go tryRunController(kubeClient, controller, ctx.Done())
	return plugin, nil
}

// tryRunController try to run the controller
// need this func because we do not want to run controller if the scheduler is not leader
func tryRunController(client kubernetes.Interface, groupController *controller.PodGroupController,
	stopCh <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	stopChPG := make(chan struct{})
	started := false
	el := resourcelock.EndpointsLock{
		EndpointsMeta: v12.ObjectMeta{
			Name:      "kube-scheduler",
			Namespace: "kube-system",
		},
		Client: client.CoreV1(),
	}

	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	for {
		select {
		case <-ticker.C:
			elRecord, _, err := el.Get()
			if err != nil || elRecord == nil {
				continue
			}

			if strings.Contains(elRecord.HolderIdentity, hostname+"_") {
				if !started && time.Now().Sub(elRecord.RenewTime.Time) < time.Duration(elRecord.
					LeaseDurationSeconds)*time.Second {
					klog.Info("Group controller started")
					go groupController.Run(10, stopChPG)
					started = true
				}
			} else {
				if started {
					klog.Info("Group controller exit")
					started = false
					stopChPG <- struct{}{}
				}
			}
		case rev := <-stopCh:
			if started {
				started = false
				stopChPG <- rev
			}
			klog.Info("Group controller exit")
			return
		}
	}
}
