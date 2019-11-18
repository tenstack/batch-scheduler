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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/sortkeys"
	gochache "github.com/patrickmn/go-cache"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8scache "k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/predicates"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	"k8s.io/kubernetes/pkg/scheduler/nodeinfo"

	pgv1 "github.com/tenstack/batch-scheduler/pkg/apis/podgroup/v1"
	pgclientset "github.com/tenstack/batch-scheduler/pkg/generated/clientset/versioned"
	pginformer "github.com/tenstack/batch-scheduler/pkg/generated/informers/externalversions/podgroup/v1"
	pglister "github.com/tenstack/batch-scheduler/pkg/generated/listers/podgroup/v1"
	"github.com/tenstack/batch-scheduler/pkg/scheduler/cache"
	errors "github.com/tenstack/batch-scheduler/pkg/util"
)

// Max score for Score
const Max = 2147483647

// ScheduleOperation defines the scheduling opeation called
type ScheduleOperation struct {
	pgClient            *pgclientset.Clientset
	frameworkHandler    framework.FrameworkHandle
	podGroupStatusCache cache.Cache
	maxScheTime         *time.Duration
	lastDeniedPG        *gochache.Cache
	lastPermittedPod    *gochache.Cache
	pgLister            pglister.PodGroupLister
	pgListerSynced      k8scache.InformerSynced
	maxFinishedPG       string
	maxPGStatus         *cache.PodGroupMatchStatus
	sync.RWMutex
}

// NewScheduleOperation create a new operation object
func NewScheduleOperation(pgClient *pgclientset.Clientset, cache cache.Cache,
	frameworkHandler framework.FrameworkHandle, maxScheTime *time.Duration, pgInformer pginformer.PodGroupInformer) *ScheduleOperation {
	so := &ScheduleOperation{
		pgClient:            pgClient,
		podGroupStatusCache: cache,
		frameworkHandler:    frameworkHandler,
		maxScheTime:         maxScheTime,
		lastDeniedPG:        gochache.New(30*time.Second, 3*time.Second),
		lastPermittedPod:    gochache.New(3*time.Second, 3*time.Second),
	}
	so.pgLister = pgInformer.Lister()
	so.pgListerSynced = pgInformer.Informer().HasSynced
	return so
}

// PodGroupStatusCache returns the podGroupStatusCache
func (sop *ScheduleOperation) PodGroupStatusCache() cache.Cache {
	return sop.podGroupStatusCache
}

// PreFilter pre-filter pod include:
// 1. cluster resource
// 2. last denied pod to increase schedule qps
// 3. last permitted pod to increase schedule qps
func (sop *ScheduleOperation) PreFilter(pod *corev1.Pod) error {
	pgName, satisfied := errors.VerifyPodLabelSatisfied(pod)
	if pgName == "" && !satisfied {
		return nil
	}
	fullName := fmt.Sprintf("%v/%v", pod.Namespace, pgName)

	_, ok := sop.lastPermittedPod.Get(string(pod.UID))
	if ok {
		return nil
	}

	pgsObj := sop.podGroupStatusCache.Get(fullName)
	if pgsObj == nil {
		return fmt.Errorf("can not found pod group: %v", fullName)
	}

	_, ok = sop.lastDeniedPG.Get(fullName)
	if ok {
		err := fmt.Errorf("pod with pgName: %v last failed in 20s, deny", fullName)
		klog.V(6).Info(err)
		return err
	}

	pgs := pgsObj.(*cache.PodGroupMatchStatus)
	if err := sop.fillOccupiedObj(pgs, pod); err != nil {
		return err
	}

	// compute resource to check if sum of cluster resource can meet required resource
	pgCache := sop.podGroupStatusCache.(*cache.PGStatusCache)
	pgCache.RLock()
	maxFinishedPG, maxPGStatus, _ := findMaxPG(pgCache)
	sop.maxFinishedPG = maxFinishedPG
	sop.maxPGStatus = maxPGStatus
	pgCache.RUnlock()
	if maxPGStatus != nil {
		klog.V(6).Infof("group %v schedule status %+v", maxFinishedPG, maxPGStatus.PodNameUIDs.Items())
	}
	if maxFinishedPG == "" || maxPGStatus == nil || maxPGStatus.PodGroup == nil {
		klog.V(5).Infof("Pod %v/%v has passed check", pod.Namespace, pod.Name)
		return nil
	}

	maxMatchedPodNodes := maxPGStatus.MatchedPodNodes.Items()

	// First scheduled pod group, set it to the current
	matched := len(maxMatchedPodNodes)
	if matched == 0 {
		maxPGStatus = pgs
		maxFinishedPG = fullName
		preAllocatedResource := getPreAllocatedResource(maxPGStatus, matched)
		if !sop.compareClusterResourceAndRequire(maxPGStatus.Pod, &preAllocatedResource, 1) {
			klog.V(5).Info("LeftResource can not satisfy preAllocationResource")
			sop.AddToDenyCache(fullName)
			return fmt.Errorf("cluster resource not enough")
		}
		klog.Warning("Returned for not found pod of PodGroupStatus")
		return nil
	}

	// max finished one, pass it
	if sop.maxFinishedPG == fullName {
		klog.V(5).Infof("Current pod group %v is the currently max one, pass it", fullName)
		klog.V(5).Infof("Current cluster resource %v", *(sop.computeClusterResource(sop.maxPGStatus.Pod)))

		return nil
	}

	preAllocatedResource := getPreAllocatedResource(maxPGStatus, matched)
	currentPodRequired := getPodResourceRequire(pod)
	preAllocatedResource.Add(currentPodRequired.ResourceList())
	klog.V(4).Infof("Current need pre-allocated resource: %+v", preAllocatedResource)
	if !sop.compareClusterResourceAndRequire(maxPGStatus.Pod, &preAllocatedResource, 0.7) {
		klog.V(5).Info("LeftResource can not satisfy preAllocationResource")
		sop.AddToDenyCache(fullName)
		return fmt.Errorf("cluster resource not enough")
	}
	return nil
}

// Filter filter pod if not fit on the node
func (sop *ScheduleOperation) Filter(pod *corev1.Pod, nodeName string) error {
	pgName, satisfied := errors.VerifyPodLabelSatisfied(pod)
	if pgName == "" && !satisfied {
		return nil
	}
	fullName := fmt.Sprintf("%v/%v", pod.Namespace, pgName)

	pgsObj := sop.podGroupStatusCache.Get(fullName)
	if pgsObj == nil {
		return fmt.Errorf("can not found pod group: %v", pgName)
	}
	pgs := pgsObj.(*cache.PodGroupMatchStatus)

	if err := sop.computeResourceSatisfied(pgs, pod, nodeName); err != nil {
		sop.AddToDenyCache(fullName)
		return err
	}
	// do not check it node have finished on one of node.
	sop.lastPermittedPod.Add(string(pod.UID), "", 2*time.Second)
	klog.V(6).Infof("Calling filter %v/%v, node %v, group %v", pod.Namespace, pod.Name, nodeName, pgName)
	return nil
}

// PreemptAddPod currently always return nil
func (sop *ScheduleOperation) PreemptAddPod(podToAdd *corev1.Pod, nodeName string) error {
	return nil
}

// PreemptRemovePod requires:
// 1. online resource could preempt online offline
// 2. offline could preempt low process and not running offline
// 3. online preempt offline
// 4. offline preempt offline
func (sop *ScheduleOperation) PreemptRemovePod(podToSchedule, podToRemove *corev1.Pod) error {
	pgNameToRemove, offlinePodToRemove := errors.VerifyPodLabelSatisfied(podToRemove)
	pgNameToSchedule, offlinePodToSchedule := errors.VerifyPodLabelSatisfied(podToSchedule)

	// 1. online resource could preempt online offline
	// 2. offline could preempt low process and not running offline

	// online preempt online
	if !offlinePodToSchedule && !offlinePodToRemove {
		return nil
	}

	// offline preempt online: forbid
	if offlinePodToSchedule && !offlinePodToRemove {
		return fmt.Errorf("offline pods %v are forbidden to preempt online %v", podToSchedule.Name, podToRemove.Name)
	}

	checkPreemption := func() (string, error) {
		fullNameToRemove := fmt.Sprintf("%v/%v", podToRemove.Namespace, pgNameToRemove)
		pgsObj := sop.podGroupStatusCache.Get(fullNameToRemove)
		if pgsObj == nil {
			return "", fmt.Errorf("can not found pod group: %v", fullNameToRemove)
		}
		// Todo: if we need forbid to preempt the max group
		//
		//maxFinishedPG, _, _ := findMaxPG(sop.podGroupStatusCache.(*cache.PGStatusCache))
		//if fullNameToRemove == maxFinishedPG {
		//	return "", fmt.Errorf("max finished pod group can not been preempted: %v", fullNameToRemove)
		//}
		//
		pgs := pgsObj.(*cache.PodGroupMatchStatus)

		if pgs.PodGroup.Status.Phase == pgv1.PodGroupScheduled ||
			pgs.PodGroup.Status.Phase == pgv1.PodGroupRunning {
			return "", fmt.Errorf("pod belongs to Scheduled or Running pod group can not be scheduled")
		}
		return fullNameToRemove, nil
	}

	fullNameToRemove, err := checkPreemption()

	// online preempt offline
	if !offlinePodToSchedule && offlinePodToRemove {
		return err
	}

	// offline preempt offline
	fullNameToSchedule := fmt.Sprintf("%v/%v", podToSchedule.Namespace, pgNameToSchedule)
	if fullNameToRemove == fullNameToSchedule {
		return fmt.Errorf("podToSchedule and podToRemove belong to same pod group, do not preempt")
	}
	if err != nil {
		return err
	}
	klog.V(5).Infof("Try to preempt pod %v/%v, schedule pod %v/%v", podToRemove.Namespace, podToRemove.Name,
		podToSchedule.Namespace, podToSchedule.Name)
	return nil
}

// Score return the score, currently it is preserved.
func (sop *ScheduleOperation) Score(pod *corev1.Pod, nodeName string) (int, error) {
	return Max, nil
}

// Permit permits a pod to run, if the minMember match, it would send a signal to chan.
func (sop *ScheduleOperation) Permit(pod *corev1.Pod, nodeName string) (bool, string, error) {
	pgName, satisfied := errors.VerifyPodLabelSatisfied(pod)
	if pgName == "" && !satisfied {
		return true, pgName, errors.ErrorNotMatched
	}
	fullName := fmt.Sprintf("%v/%v", pod.Namespace, pgName)
	pgsObj := sop.podGroupStatusCache.Get(fullName)
	if pgsObj == nil {
		return false, pgName, fmt.Errorf("can not found pod group: %v", pgName)
	}
	pgs := pgsObj.(*cache.PodGroupMatchStatus)
	if pgs.PodGroup.Status.Phase == pgv1.PodGroupPending {
		pgs.PodGroup.Status.Phase = pgv1.PodGroupPreScheduling
	}
	klog.V(5).Infof("Desired group %v phase: %v", pgs.PodGroup.Name, pgs.PodGroup.Status.Phase)

	pair := cache.PodNodePair{
		PodName: fmt.Sprintf("%v/%v", pod.Namespace, pod.Name),
		Node:    nodeName,
	}

	waitTime := errors.GetWaitTimeDuration(pgs.PodGroup, sop.maxScheTime)
	pgs.MatchedPodNodes.Set(string(pod.UID), &pair, waitTime)
	oldUID, found := pgs.PodNameUIDs.Get(fmt.Sprintf("%v/%v", pod.Namespace, pod.Name))
	// pod has been scheduled ever
	if found {
		// delete the expired one
		pgs.MatchedPodNodes.Delete(oldUID.(string))
	} else {
		// newly add one
		klog.V(4).Infof("Add new cache data %v", pod.UID)
	}
	pgs.PodNameUIDs.Set(fmt.Sprintf("%v/%v", pod.Namespace, pod.Name), string(pod.UID), waitTime)

	klog.V(5).Infof("Current pod group state: %+v", pgs.PodGroup)
	ready := uint32(len(pgs.MatchedPodNodes.Items())) >= pgs.PodGroup.Spec.MinMember-pgs.PodGroup.Status.Scheduled
	if ready {
		pgs.Scheduled = true
		return true, pgName, nil
	}
	return false, pgName, errors.ErrorWaiting
}

// PostBind is used for send metrics and update to api if all finish
func (sop *ScheduleOperation) PostBind(pod *corev1.Pod, nodeName string) {
	pgName, satisfied := errors.VerifyPodLabelSatisfied(pod)
	if pgName == "" && !satisfied {
		return
	}

	fullName := fmt.Sprintf("%v/%v", pod.Namespace, pgName)
	sop.Lock()
	defer sop.Unlock()
	pgsObj := sop.podGroupStatusCache.Get(fullName)
	if pgsObj == nil {
		return
	}
	pgs := pgsObj.(*cache.PodGroupMatchStatus)
	pgCopy := pgs.PodGroup.DeepCopy()
	pgCopy.Status.Scheduled++

	if pgCopy.Status.Scheduled >= pgs.PodGroup.Spec.MinMember {
		pgCopy.Status.Phase = pgv1.PodGroupScheduled
	} else {
		pgCopy.Status.Phase = pgv1.PodGroupScheduling
		if pgCopy.Status.ScheduleStartTime.IsZero() {
			pgCopy.Status.ScheduleStartTime = v1.Time{Time: time.Now()}
		}
	}

	if pgCopy.Status.Phase != pgs.PodGroup.Status.Phase {

		pg, err := sop.pgClient.BatchV1().PodGroups(pgCopy.Namespace).Get(pgCopy.Name, v1.GetOptions{})
		if err != nil {
			klog.Error(err)
			return
		}

		patch, err := errors.CreateMergePatch(pg, pgCopy)
		if err != nil {
			return
		}

		pg, err = sop.pgClient.BatchV1().PodGroups(pg.Namespace).Patch(pg.Name, types.MergePatchType, patch)
		if err != nil {
			return
		}

		pgs.PodGroup.Status.Phase = pg.Status.Phase
	}

	pgs.PodGroup.Status.Scheduled = pgCopy.Status.Scheduled

	return
}

// Compare returns the true or false, base on
// 1. priority
// 2. podGroup creation time
// 3. pod creation time
func (sop *ScheduleOperation) Compare(podInfo1, podInfo2 interface{}) bool {
	// Sort pod also record to progress
	pInfo1 := podInfo1.(*framework.PodInfo)
	pInfo2 := podInfo2.(*framework.PodInfo)
	prio1 := podutil.GetPodPriority(pInfo1.Pod)
	prio2 := podutil.GetPodPriority(pInfo2.Pod)

	pgName1, _ := errors.VerifyPodLabelSatisfied(pInfo1.Pod)
	pgName2, _ := errors.VerifyPodLabelSatisfied(pInfo2.Pod)

	compare := func() bool {
		if prio1 > prio2 {
			return true
		}

		if prio1 == prio2 {
			if pgName1 == "" && pgName2 == "" {
				return pInfo1.Timestamp.Before(pInfo2.Timestamp)
			}

			if pgName1 == "" {
				return true
			}
			if pgName2 == "" {
				return false
			}
		}
		pg1, err1 := sop.pgLister.PodGroups(pInfo1.Pod.Namespace).Get(pgName1)
		pg2, err2 := sop.pgLister.PodGroups(pInfo2.Pod.Namespace).Get(pgName2)
		if err1 != nil || err2 != nil {
			return false
		}
		if prio1 == prio2 && pg1.CreationTimestamp.Before(&pg2.CreationTimestamp) {
			return true
		}

		if prio1 == prio2 && pg1.CreationTimestamp.Equal(&pg2.CreationTimestamp) && pgName1 > pgName2 {
			return true
		}
		return prio1 == prio2 && pg1.CreationTimestamp.Equal(&pg2.CreationTimestamp) && pgName1 == pgName2 && pInfo1.
			Timestamp.Before(pInfo2.Timestamp)
	}
	return compare()
}

// GetPodNodePairs returns the pod-node paired to be scheduled.
func (sop *ScheduleOperation) GetPodNodePairs(fullName string) *gochache.Cache {
	pgs := sop.podGroupStatusCache.Get(fullName)
	if pgs == nil {
		return nil
	}
	return pgs.(*cache.PodGroupMatchStatus).MatchedPodNodes
}

// AddToDenyCache add podGroup to the back list cache
func (sop *ScheduleOperation) AddToDenyCache(fullName string) {
	sop.lastDeniedPG.Add(fullName, "", 20*time.Second)
}

// GetPodNameUIDs returns podName and uid pairs.
func (sop *ScheduleOperation) GetPodNameUIDs(fullName string) *gochache.Cache {
	pgs := sop.podGroupStatusCache.Get(fullName)
	if pgs == nil {
		return nil
	}
	return pgs.(*cache.PodGroupMatchStatus).PodNameUIDs
}

func (sop *ScheduleOperation) getLeftResource(nodeName string) *nodeinfo.Resource {
	snapShot := sop.frameworkHandler.SnapshotSharedLister()
	if snapShot == nil || snapShot.NodeInfos() == nil {
		return nil
	}
	klog.V(6).Infof("NodeInfoSnapshot() %+v", snapShot)
	info, err := snapShot.NodeInfos().Get(nodeName)
	if err != nil {
		return nil
	}

	if info == nil {
		return nil
	}

	var leftResource nodeinfo.Resource
	allocatable := info.AllocatableResource()
	requested := info.RequestedResource()

	podCount := requested.AllowedPodNumber
	if podCount == 0 {
		podCount = len(info.Pods())
	}

	leftResource.MilliCPU = allocatable.MilliCPU - requested.MilliCPU
	leftResource.AllowedPodNumber = allocatable.AllowedPodNumber - podCount
	leftResource.Memory = allocatable.Memory - requested.Memory
	leftResource.EphemeralStorage = allocatable.EphemeralStorage - requested.EphemeralStorage

	leftResourceCopy := leftResource.Clone()
	for k, v1 := range leftResourceCopy.ScalarResources {
		v2, ok := allocatable.ScalarResources[k]
		if !ok {
			continue
		}
		leftResource.ScalarResources[k] = v1 - v2
	}

	return &leftResource
}

func (sop *ScheduleOperation) fillOccupiedObj(pgs *cache.PodGroupMatchStatus, pod *corev1.Pod) error {
	if pgs == nil || pgs.PodGroup == nil {
		return fmt.Errorf("PodGroupMatchStatus is nil")
	}
	fullName := fmt.Sprintf("%v/%v", pod.Namespace, pgs.PodGroup.Name)
	refs := make([]string, 0)
	for _, ownerRef := range pod.OwnerReferences {
		refs = append(refs, string(ownerRef.UID))
	}
	if pgs.Pod == nil {
		pgs.Pod = pod
	}
	if pgs.PodGroup.Spec.MinResources == nil {
		// initialize pod group resource
		reql := getPodResourceRequire(pod).ResourceList()
		pgs.PodGroup.Spec.MinResources = &reql
	}
	if pgs.PodGroup.Status.OccupiedBy == "" {
		// if do not have refs or initialize, return nil
		if len(refs) != 0 {
			// initialize pod group
			sortkeys.Strings(refs)
			pgs.PodGroup.Status.OccupiedBy = strings.Join(refs, ",")
		}
		return nil
	}
	// if refs not match, return error
	if len(refs) == 0 {
		return fmt.Errorf("pod group %s has been occupied by %v", fullName, pgs.PodGroup.Status.OccupiedBy)
	}
	sortkeys.Strings(refs)
	if strings.Join(refs, ",") != pgs.PodGroup.Status.OccupiedBy {
		return fmt.Errorf("pod group has been occupied by %v", pgs.PodGroup.Status.OccupiedBy)
	}
	return nil
}

func (sop *ScheduleOperation) computeResourceSatisfied(podPGS *cache.PodGroupMatchStatus, pod *corev1.Pod,
	nodeName string) error {
	// sum resources
	// pre-filter would filter some pod according to cluster resource, so do not need check again.
	// case1. if pod is the current max finished group, turn true, else return false.
	// case2. if can satisfied max one + current one, return true
	// case3. if any one pod in the max finished group requires more resource than node, but node can satisfy
	// the current one, return true, else return false

	fullName := fmt.Sprintf("%v/%v", pod.Namespace, podPGS.PodGroup.Name)
	var maxSingleRequired *nodeinfo.Resource
	if sop.maxPGStatus.PodGroup.Spec.MinResources != nil {
		maxSingleRequired = &nodeinfo.Resource{}
		maxSingleRequired.Add(*sop.maxPGStatus.PodGroup.Spec.MinResources)
	}

	// case1
	if sop.maxFinishedPG == fullName {
		klog.V(5).Infof("Current pod group %v is the currently max one, pass it", podPGS.PodGroup.Name)

		return nil
	}

	if klog.V(5) {
		klog.Infof("Current cluster resource %v", *(sop.computeClusterResource(sop.maxPGStatus.Pod)))
	}

	// do not check current pod required, it has checked
	if maxSingleRequired == nil {
		return nil
	}
	nodeLeftResource := sop.getLeftResource(nodeName)
	if nodeLeftResource == nil {
		return fmt.Errorf("SnapShot not initialized")
	}

	// case2
	currentPodRequire := getPodResourceRequire(pod)
	currentPodRequire.Add(maxSingleRequired.ResourceList())
	if compareResourceAndRequire(nodeLeftResource, currentPodRequire) {
		return nil
	}

	// case3
	if !compareResourceAndRequire(nodeLeftResource, maxSingleRequired) {
		klog.V(5).Info("LeftResource can not satisfy the pod of the max finished one, meet the current one")
		return nil
	}
	klog.V(5).Infof("Pod %v can not scheduled: resource not enough", pod.Name)
	return errors.ErrorResourceNotEnough
}

func (sop *ScheduleOperation) computeClusterResource(pod *corev1.Pod) *nodeinfo.Resource {
	snapShotList, err := sop.frameworkHandler.SnapshotSharedLister().NodeInfos().List()
	if err != nil || snapShotList == nil {
		return nil
	}
	klog.V(6).Infof("NodeInfoSnapshot() %+v", snapShotList)
	var leftResources nodeinfo.Resource
	for k, info := range snapShotList {
		if info == nil {
			continue
		}

		if info.Node() == nil {
			klog.Warningf("Node: %v can not found", k)
			continue
		}

		if info.Node().Spec.Unschedulable {
			continue
		}

		leftResource := singleNodeResource(info, pod, 1)

		leftResources.Add(leftResource.ResourceList())

	}
	return &leftResources
}

func (sop *ScheduleOperation) compareClusterResourceAndRequire(pod *corev1.Pod,
	reqResource *nodeinfo.Resource, percent float32) bool {
	snapShotList, err := sop.frameworkHandler.SnapshotSharedLister().NodeInfos().List()
	if err != nil || snapShotList == nil {
		return false
	}
	klog.V(6).Infof("NodeInfoSnapshot() %+v", snapShotList)
	var leftResources nodeinfo.Resource
	count := 0
	for k, info := range snapShotList {
		count++
		if info == nil {
			continue
		}

		if info.Node() == nil {
			klog.Warningf("Node: %v can not found", k)
			continue
		}

		if info.Node().Spec.Unschedulable {
			continue
		}

		leftResource := singleNodeResource(info, pod, percent)

		leftResources.Add(leftResource.ResourceList())

		if compareResourceAndRequire(&leftResources, reqResource) {
			klog.V(5).Infof("Current left %v nodes resources %+v \nRequired resource %+v", count,
				leftResources, *reqResource)
			return true
		}
		klog.V(5).Infof("Current left %v nodes resources %+v \nRequired resource %+v", count,
			leftResources, *reqResource)
	}
	return false
}

func singleNodeResource(info *nodeinfo.NodeInfo, pod *corev1.Pod, percent float32) *nodeinfo.Resource {
	leftResource := nodeinfo.Resource{
		ScalarResources: make(map[corev1.ResourceName]int64),
	}
	satisfied := false
	if _, err := info.Taints(); err != nil {
		return &leftResource
	}
	satisfied = checkFit(pod, info)
	if !satisfied {
		return &leftResource
	}

	allocatable := info.AllocatableResource()
	requested := info.RequestedResource()

	podCount := requested.AllowedPodNumber
	if podCount == 0 {
		podCount = len(info.Pods())
	}

	// reserve `percent` resource
	leftResource.AllowedPodNumber = int(float32(allocatable.AllowedPodNumber)*percent) - podCount
	leftResource.MilliCPU = int64(float32(allocatable.MilliCPU)*percent) - requested.MilliCPU
	leftResource.Memory = int64(float32(allocatable.Memory)*percent) - requested.Memory
	leftResource.EphemeralStorage = int64(float32(allocatable.EphemeralStorage)*percent) - requested.EphemeralStorage

	// calculate extend resources
	for k, allocatableEx := range allocatable.ScalarResources {
		requestEx, ok := requested.ScalarResources[k]
		if !ok {
			continue
		}
		leftResource.ScalarResources[k] = int64(float32(allocatableEx)*percent) - requestEx
	}
	return &leftResource
}

func compareResourceAndRequire(leftResource, req *nodeinfo.Resource) bool {
	if leftResource.Memory < req.Memory {
		return false
	}
	if leftResource.MilliCPU < req.MilliCPU {
		return false
	}
	if leftResource.EphemeralStorage < req.EphemeralStorage {
		return false
	}
	// AllowedPodNumber seems not correct, so we need check again
	if leftResource.AllowedPodNumber < req.AllowedPodNumber {
		return false
	}
	for k, v1 := range req.ScalarResources {
		v2, ok := leftResource.ScalarResources[k]
		if !ok {
			if v1 != 0 {
				return false
			}
			continue
		}
		if v1 > v2 {
			return false
		}
	}
	return true
}

func findMaxPG(pgCache *cache.PGStatusCache) (maxFinishedPG string,
	maxPGStatus *cache.PodGroupMatchStatus, maxFinished uint32) {
	for pgName, pgs := range pgCache.PGStatusMap {
		// if we have meet min request, make other pods priority
		var finished uint32 = 0
		if pgs.Scheduled {
			continue
		}
		if pgs.Pod == nil {
			continue
		}
		if pgs.PodGroup.Spec.MinMember-pgs.PodGroup.
			Status.Scheduled <= 0 {
			finished = 0
		} else {
			finished = (uint32(len(pgs.MatchedPodNodes.Items())) + pgs.
				PodGroup.Status.Scheduled) * 1000 / (pgs.PodGroup.Spec.MinMember)
			klog.V(5).Infof("group %v schedule progress %d/100", pgName, finished/10)
		}

		if finished > maxFinished {
			maxFinished = finished
			maxFinishedPG = pgName
			maxPGStatus = pgs
		} else if finished == maxFinished {
			// avoid maxPG is always the finished one
			// 1. if max is nil, set the value
			// 2. if the max is scheduled and running, make the not scheduled first
			if maxPGStatus == nil ||
				maxPGStatus.PodGroup.Status.Scheduled >= maxPGStatus.PodGroup.Spec.MinMember &&
					pgs.PodGroup.Status.Scheduled == 0 {
				maxFinished = finished
				maxFinishedPG = pgName
				maxPGStatus = pgs
			}
		}
	}
	return
}

func checkFit(pod *corev1.Pod, info *nodeinfo.NodeInfo) bool {
	predicateFails := make([]predicates.PredicateFailureReason, 0)
	fit, reasons, err := predicates.PodMatchNodeSelector(pod, nil, info)
	if err != nil {
		return false
	}
	if !fit {
		predicateFails = append(predicateFails, reasons...)
	}

	fit, reasons, err = predicates.PodToleratesNodeTaints(pod, nil, info)
	if err != nil {
		return false
	}
	if !fit {
		predicateFails = append(predicateFails, reasons...)
	}
	return len(predicateFails) == 0
}

func getPodResourceRequire(pod *corev1.Pod) *nodeinfo.Resource {
	var currentPodRequired nodeinfo.Resource
	// Compute current pod required
	for _, c := range pod.Spec.Containers {
		if c.Resources.Limits != nil {
			currentPodRequired.Add(c.Resources.Limits)
		} else {
			currentPodRequired.Add(c.Resources.Requests)
		}
	}
	return &currentPodRequired
}

func getPreAllocatedResource(maxPGStatus *cache.PodGroupMatchStatus, matched int) (preAllocatedResource nodeinfo.
Resource) {
	notFinished := 0
	scheduled := int(maxPGStatus.PodGroup.Status.Scheduled)
	if matched != 0 {
		notFinished = int(maxPGStatus.PodGroup.Spec.MinMember) - matched
	} else {
		// recover from stop
		notFinished = int(maxPGStatus.PodGroup.Spec.MinMember) - scheduled
	}
	for i := 0; i < notFinished; i++ {
		if maxPGStatus.PodGroup.Spec.MinResources != nil {
			preAllocatedResource.Add(*maxPGStatus.PodGroup.Spec.MinResources)
		}
	}
	if preAllocatedResource.AllowedPodNumber == 0 {
		preAllocatedResource.AllowedPodNumber = int(maxPGStatus.PodGroup.Spec.MinMember) + 1
	}
	return
}
