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

package cache

import (
	"sync"

	gochache "github.com/patrickmn/go-cache"
	v12 "k8s.io/api/core/v1"

	v1 "github.com/tenstack/batch-scheduler/pkg/apis/podgroup/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Cache provides func Get, Set, Delete to operation cache
type Cache interface {
	Get(string) Object
	Set(string, Object)
	Delete(string)
}

// Object is A abstract interface
type Object interface {
}

var _ Cache = &PGStatusCache{}

var _ Object = &PodGroupMatchStatus{}

// PGStatusCache provide safe function Get, Set, Delete.
type PGStatusCache struct {
	sync.RWMutex
	// NodeDiskStatus maps podGroupName to its DiskStatus
	PGStatusMap map[string]*PodGroupMatchStatus
}

// PodGroupMatchStatus helps record pod group status
type PodGroupMatchStatus struct {
	// used to lock map
	CountLock sync.RWMutex
	PodGroup  *v1.PodGroup
	// store permitted pod node map with ttl
	MatchedPodNodes *gochache.Cache
	// store permitted pod name and uuid map with ttl
	PodNameUIDs *gochache.Cache
	// failed pods
	Failed map[string]string
	// succeed pods
	Succeed map[string]string
	Pod     *v12.Pod
	// Scheduled marks if has scheduled
	Scheduled bool
}

// PodNodePair is a pair contains pod name and node
type PodNodePair struct {
	PodName string
	Node    string
}

// NewPGStatusCache initializers pod status cache
func NewPGStatusCache() *PGStatusCache {
	return &PGStatusCache{
		PGStatusMap: map[string]*PodGroupMatchStatus{},
	}
}

// DeepCopy copies the pod node pair
func DeepCopy(pairs map[types.UID]*PodNodePair) map[types.UID]*PodNodePair {
	newPair := make(map[types.UID]*PodNodePair)
	for id, pair := range pairs {
		pairObj := *pair
		pairObjCopy := pairObj
		newPair[id] = &pairObjCopy
	}
	return newPair
}

// Get cache according to podGroupName
func (psc *PGStatusCache) Get(podGroupName string) Object {
	psc.RLock()
	defer psc.RUnlock()
	pg, found := psc.PGStatusMap[podGroupName]
	if found == false {
		return nil
	}
	return pg
}

//Set node status to the cache
func (psc *PGStatusCache) Set(podGroupName string, pg Object) {
	psc.Lock()
	psc.PGStatusMap[podGroupName] = pg.(*PodGroupMatchStatus)
	psc.Unlock()
}

// Delete Node status from cache
func (psc *PGStatusCache) Delete(podGroupName string) {
	psc.Lock()
	delete(psc.PGStatusMap, podGroupName)
	psc.Unlock()
}

/*
var _ Cache = &NodeResourceCache{}

var _ Object = &nodeinfo.Resource{}

func NewNodeResourceCache() *NodeResourceCache {
	return &NodeResourceCache{
		Resources: map[string]*nodeinfo.Resource{},
	}
}

type NodeResourceCache struct {
	sync.RWMutex
	//map[nodeName] Pre-allocationIno
	Resources map[string]*nodeinfo.Resource
}

func (n *NodeResourceCache) Get(node string) Object {
	n.RLock()
	defer n.RUnlock()
	resource, found := n.Resources[node]
	if !found {
		return nil
	}
	return resource
}

func (n *NodeResourceCache) Set(node string, obj Object) {
	n.Lock()
	n.Resources[node] = obj.(*nodeinfo.Resource)
	n.Unlock()
}

func (n *NodeResourceCache) Delete(node string) {
	n.Lock()
	delete(n.Resources, node)
	defer n.Unlock()
}
*/
