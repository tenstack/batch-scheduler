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

package controller

import (
	"reflect"
	"time"

	gochache "github.com/patrickmn/go-cache"
	"github.com/tenstack/batch-scheduler/pkg/util"
	v1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"

	pgv1 "github.com/tenstack/batch-scheduler/pkg/apis/podgroup/v1"
	pgclientset "github.com/tenstack/batch-scheduler/pkg/generated/clientset/versioned"
	pginformer "github.com/tenstack/batch-scheduler/pkg/generated/informers/externalversions/podgroup/v1"
	pglister "github.com/tenstack/batch-scheduler/pkg/generated/listers/podgroup/v1"
	pgcache "github.com/tenstack/batch-scheduler/pkg/scheduler/cache"
)

// PodGroupController is a controller that process pod groups using provided Handler interface
type PodGroupController struct {
	client         kubernetes.Interface
	eventRecorder  record.EventRecorder
	pgQueue        workqueue.RateLimitingInterface
	pgLister       pglister.PodGroupLister
	pgListerSynced cache.InformerSynced
	pgClient       *pgclientset.Clientset
	cache          pgcache.Cache
	rejectPod      func(types.UID)
	addToBackOff   func(string)
}

// NewPodGroupController returns a new *PodGroupController
func NewPodGroupController(client kubernetes.Interface,
	pgInformer pginformer.PodGroupInformer,
	pgRateLimiter workqueue.RateLimiter,
	pgClient *pgclientset.Clientset,
	pgCache pgcache.Cache, rejectPod func(types.UID),
	addToBackOff func(string)) *PodGroupController {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: client.CoreV1().Events(v1.NamespaceAll)})
	var eventRecorder record.EventRecorder
	eventRecorder = broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "batch-scheduling"})

	ctrl := &PodGroupController{
		client:        client,
		eventRecorder: eventRecorder,
		pgQueue:       workqueue.NewNamedRateLimitingQueue(pgRateLimiter, "batch-scheduling-queue"),
		cache:         pgCache,
		rejectPod:     rejectPod,
		addToBackOff:  addToBackOff,
	}

	pgInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.pgAdded,
		UpdateFunc: ctrl.pgUpdated,
		DeleteFunc: ctrl.pgDelete,
	})
	ctrl.pgLister = pgInformer.Lister()
	ctrl.pgListerSynced = pgInformer.Informer().HasSynced
	ctrl.pgClient = pgClient
	return ctrl
}

// Run starts listening on channel events
func (ctrl *PodGroupController) Run(workers int, stopCh <-chan struct{}) {
	defer ctrl.pgQueue.ShutDown()

	klog.Infof("Starting batch scheduler")
	defer klog.Infof("Shutting batch scheduler")

	if !cache.WaitForCacheSync(stopCh, ctrl.pgListerSynced, ctrl.pgListerSynced) {
		klog.Errorf("Cannot sync caches")
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(ctrl.sync, 0, stopCh)
	}

	<-stopCh
}

// pgAdded reacts to a PG creation
func (ctrl *PodGroupController) pgAdded(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	pg := obj.(*pgv1.PodGroup)
	if pg.Status.Phase == pgv1.PodGroupFinished || pg.Status.Phase == pgv1.PodGroupFailed {
		return
	}
	// If startScheduleTime - createTime > 2days, do not enqueue again because pod may have be GC
	if pg.Status.Scheduled == pg.Spec.MinMember && pg.Status.Running == 0 &&
		pg.Status.ScheduleStartTime.Sub(pg.CreationTimestamp.Time) > 48*time.Hour {
		return
	}
	klog.Info("enqueue ", "key ", key)
	ctrl.pgQueue.Add(key)
}

// pgUpdated reacts to a PG update
func (ctrl *PodGroupController) pgUpdated(old, new interface{}) {
	ctrl.pgAdded(new)
}

// pgDelete reacts to a PG update
func (ctrl *PodGroupController) pgDelete(new interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(new)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	klog.Info("enqueue ", "key ", key)
	ctrl.cache.Delete(key)
	klog.V(3).Infof("pg %q delete change", key)
}

// syncPG deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *PodGroupController) sync() {
	keyObj, quit := ctrl.pgQueue.Get()
	if quit {
		return
	}
	defer ctrl.pgQueue.Done(keyObj)

	key := keyObj.(string)
	namespace, pgName, err := cache.SplitMetaNamespaceKey(key)
	klog.V(4).Infof("Started PG processing %q", pgName)

	// get PG to process
	pg, err := ctrl.pgLister.PodGroups(namespace).Get(pgName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			pg, err = ctrl.pgClient.BatchV1().PodGroups(namespace).Get(pgName, metav1.GetOptions{})
			if err != nil && apierrs.IsNotFound(err) {
				// PG was deleted in the meantime, ignore.
				klog.V(3).Infof("PG %q deleted", pgName)
				ctrl.cache.Delete(key)
				return
			}
		}
		klog.Errorf("Error getting PodGroup %q: %v", pgName, err)
		ctrl.pgQueue.AddRateLimited(keyObj)
		return
	}
	ctrl.syncHandler(pg)
}

// syncHandle syncs pod group and convert status
func (ctrl *PodGroupController) syncHandler(pg *pgv1.PodGroup) {

	key, err := cache.MetaNamespaceKeyFunc(pg)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	defer func() {
		if err != nil {
			ctrl.pgQueue.AddRateLimited(key)
			return
		}
	}()

	pgsObj := ctrl.cache.Get(key)
	if pgsObj == nil {
		pgsObj = ctrl.initPodGroupMatchStatus(pg, key)
	}
	pgCopy := pg.DeepCopy()
	if string(pgCopy.Status.Phase) == "" {
		pgCopy.Status.Phase = pgv1.PodGroupPending
	} else if pgCopy.Status.Phase == pgv1.PodGroupPending && !pgCopy.Status.ScheduleStartTime.IsZero() {
		// recover from abnormal exit
		selector := labels.Set(map[string]string{util.PodGroupLabel: pgCopy.Name}).AsSelector()
		options := metav1.ListOptions{LabelSelector: selector.String()}
		var pods *v1.PodList
		pods, err = ctrl.client.CoreV1().Pods(pgCopy.Namespace).List(options)
		if err != nil {
			return
		}
		pgCopy.Status.Scheduled = uint32(len(pods.Items))
		if pgCopy.Status.Scheduled > 0 && !reflect.DeepEqual(pg, pgCopy) {
			patch, err := util.CreateMergePatch(pg, pgCopy)
			if err != nil {
				return
			}

			pg, err = ctrl.pgClient.BatchV1().PodGroups(pg.Namespace).Patch(pg.Name, types.MergePatchType, patch)
			if err != nil {
				return
			}
		}
	}

	pgs := pgsObj.(*pgcache.PodGroupMatchStatus)
	pgs.PodGroup.Status = pgCopy.Status
	ctrl.cache.Set(key, pgs)
	// If startScheduleTime - createTime > 2days, do not enqueue again because pod may have be GC
	if pgCopy.Status.Scheduled == pgCopy.Spec.MinMember && pgCopy.Status.Running == 0 &&
		pgCopy.Status.ScheduleStartTime.Sub(pgCopy.CreationTimestamp.Time) > 48*time.Hour {
		return
	}

	// Add PodGroupScheduling batch to queue to avoid scheduling stop when postBind,
	// so we do not know how many pods have been scheduled
	if pgCopy.Status.Phase == pgv1.PodGroupScheduled || pgCopy.Status.Phase == pgv1.PodGroupRunning || pgCopy.Status.
		Phase == pgv1.PodGroupScheduling {
		selector := labels.Set(map[string]string{util.PodGroupLabel: pgCopy.Name}).AsSelector()
		options := metav1.ListOptions{LabelSelector: selector.String()}
		var pods *v1.PodList
		pods, err = ctrl.client.CoreV1().Pods(pgCopy.Namespace).List(options)
		if err != nil {
			return
		}
		pgs.CountLock.Lock()
		var notPending uint32 = 0
		var running uint32 = 0
		if pods != nil {
			for _, pod := range pods.Items {
				switch pod.Status.Phase {
				case v1.PodRunning:
					running++
				case v1.PodSucceeded:
					pgs.Succeed[string(pod.UID)] = ""
				case v1.PodFailed:
					pgs.Failed[string(pod.UID)] = ""
				}

				// to avoid schedule stop when postBind, so when do not know how many pods have scheduled
				if pod.Status.Phase != v1.PodPending {
					notPending++
				}

			}
		}
		pgCopy.Status.Failed = uint32(len(pgs.Failed))
		pgCopy.Status.Succeeded = uint32(len(pgs.Succeed))
		pgCopy.Status.Running = running

		if notPending > pgCopy.Status.Scheduled {
			pgCopy.Status.Scheduled = notPending

		}
		pgs.CountLock.Unlock()

		// recover from exit
		if notPending < pgCopy.Spec.MinMember && notPending != 0 {
			pgCopy.Status.Scheduled = notPending
			pgCopy.Status.Phase = pgv1.PodGroupScheduling
		}

		if pgCopy.Status.Succeeded+pgCopy.Status.Running >= pg.Spec.MinMember {
			pgCopy.Status.Phase = pgv1.PodGroupRunning
		}
		// Final state of pod group
		if pgCopy.Status.Failed != 0 && pgCopy.Status.Failed+pgCopy.Status.Running+pgCopy.Status.Succeeded >= pg.Spec.
			MinMember {
			pgCopy.Status.Phase = pgv1.PodGroupFailed
		}
		if pgCopy.Status.Succeeded >= pg.Spec.MinMember {
			pgCopy.Status.Phase = pgv1.PodGroupFinished
		}
	}
	if !reflect.DeepEqual(pg, pgCopy) {
		var patch []byte
		patch, err = util.CreateMergePatch(pg, pgCopy)
		if err != nil {
			return
		}

		pg, err = ctrl.pgClient.BatchV1().PodGroups(pg.Namespace).Patch(pg.Name, types.MergePatchType, patch)
		if err != nil {
			return
		}
		if pg.Status.Phase == pgv1.PodGroupFinished || pg.Status.Phase == pgv1.PodGroupFailed {
			ctrl.cache.Delete(key)
		}
		pgs.PodGroup.Status = pg.Status
		ctrl.pgQueue.Forget(pg)
	}
	ctrl.pgQueue.AddRateLimited(key)
}

// initPodGroupMatchStatus init pod groups
func (ctrl *PodGroupController) initPodGroupMatchStatus(pg *pgv1.PodGroup, key string) *pgcache.PodGroupMatchStatus {
	pgs := &pgcache.PodGroupMatchStatus{
		PodGroup:        pg,
		MatchedPodNodes: gochache.New(1*time.Minute, 2*time.Minute),
		PodNameUIDs:     gochache.New(1*time.Minute, 2*time.Minute),
		Failed:          make(map[string]string),
		Succeed:         make(map[string]string),
	}
	pgs.PodNameUIDs.OnEvicted(func(s string, i interface{}) {
		klog.V(4).Infof("Evict triggered group %v", key)
		for podID := range pgs.MatchedPodNodes.Items() {
			klog.Infof("Foreach %v", podID)
			ctrl.rejectPod(types.UID(podID))
			if pgs.MatchedPodNodes != nil {
				pgs.MatchedPodNodes.Delete(podID)
			}
		}
		pgs.PodNameUIDs.Flush()
		ctrl.addToBackOff(key)
	})
	return pgs
}
