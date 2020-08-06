# Batch-scheduler 

## Background

Currently, through the default scheduler of Kubernetes, we cannot ensure a group of pods scheduled at the same time
. Under some scene, it would waste resources since some pods need work together, like `spark`, `tensorflow` and so on
. So, batch-scheduler is aimed at solving the issue.

## Method

### Features

- lightweight
- no resource race
- gang scheduling

### Implementation

Based on the latest [scheduling framework](https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework), we designed the scheduler. So only one scheduler is needed to run  in the
 cluster, which makes sure `resource race` would not happen. 
 
 This scheduler also makes sure gang scheduling, e.g.
 - scene1

A group consists of 5 pods. The batch-scheduler would not schedule any pod until enough resources are found.

 - scene2
Only 6 cpu exist in the cluster. Two groups require 5 cpus and 5 cpus are submitted, then only one and at least one
 group would be scheduled.

How to keep light-weight. We named a CRD [PodGroup](./pkg/apis/podgroup/v1/types.go). When we would to running a group
 of pods, just need submit a `PodGroup`, e.g. `group1` into the cluster. The pods needs to run as a group should only add a label named: `group.batch
 .scheduler.tencent.com: group1` 

### Main Progress

1. `PreFilter`: Compute resource requirements before we start predicts for a pod. If a pod is not permitted, we add it
 to freeze cache, then the pods belong to the same group would be rejected directly.
 
2. `Less`: this interface decides the sequence of pods. Currently, pods having higher `Priority` would be scheduled
 first. If pods have same the priority, PodGroup Creation time would be compared.

3. `Permit`: it is used for approving a pod or denying one. If a pod can be scheduled, but the number of pods belongs
 to the same group has not reached the min requested, it returns `Wait`. If a pod cannot be scheduled, it returns
  `Unschedulable`.

4. It is better to set MaxScheduleTime for a PodGroup. If one of the pods belong to the same PodGroup times out
, other pods would also be rejected.

## Build

```
# git clone git@github.com/tenstack/batch-scheduler.git
# make build
```

## Deploy

- Deploy CRD

```
# cd deploy
# kubectl apply -f deploy crd.yaml
```
- Configuration

Default config has been written, but `kube_config` in it should be changed to your self stored.

- Deploy batch-scheduler

```$xslt
# cd deploy
# bash start.sh
```

## Example

This example shows the resource race scene. Only 8 cpu exist in the cluster, and 0.9 has been occupied.

```$xslt
Allocated resources:
  (Total limits may be over 100 percent, i.e., overcommitted.)
  Resource           Requests    Limits
  --------           --------    ------
  cpu                900m (11%)  0 (0%)
  memory             140Mi (0%)  340Mi (2%)
  ephemeral-storage  0 (0%)      0 (0%)
Events:              <none>
```

- Yaml file named `sts-group-valid-race.yaml` is as follow
```$xslt
apiVersion: batch.scheduler.tencent.com/v1
kind: PodGroup
metadata:
  name: group1
  namespace: default
spec:
  minMember: 5
---
apiVersion: batch.scheduler.tencent.com/v1
kind: PodGroup
metadata:
  name: group2
  namespace: default
spec:
  minMember: 5
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: web-group-race1
spec:
  selector:
    matchLabels:
      app: nginx
  podManagementPolicy: Parallel
  serviceName: "nginx"
  replicas: 5
  template:
    metadata:
      labels:
        group.batch.scheduler.tencent.com: "group1"
        app: nginx
        type: node
    spec:
      containers:
      - name: nginx
        image: nginx
        ports:
        - containerPort: 80
          name: web
        resources:
          limits:
            cpu: "1"
          requests:
            cpu: "1"
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: web-group-race2
spec:
  selector:
    matchLabels:
      app: nginx
  podManagementPolicy: Parallel
  serviceName: "nginx"
  replicas: 5
  template:
    metadata:
      labels:
        group.batch.scheduler.tencent.com: "group2"
        app: nginx
        type: node
    spec:
      containers:
      - name: nginx
        image: nginx
        ports:
        - containerPort: 80
          name: web
        resources:
          limits:
            cpu: "1"
          requests:
            cpu: "1"
```

- Submit it

```$xslt
# kubectl apply -f sts-group-valid-race.yaml
```
- Results
```$xslt
[root@cwd-dev ~]# kubectl get pod
NAME                            READY   STATUS              RESTARTS   AGE
web-group-race1-0               0/1     ContainerCreating   0          16s
web-group-race1-1               0/1     ContainerCreating   0          16s
web-group-race1-2               0/1     ContainerCreating   0          16s
web-group-race1-3               1/1     Running             0          16s
web-group-race1-4               0/1     ContainerCreating   0          16s
web-group-race2-0               0/1     Pending             0          16s
web-group-race2-1               0/1     Pending             0          16s
web-group-race2-2               0/1     Pending             0          16s
web-group-race2-3               0/1     Pending             0          16s
web-group-race2-4               0/1     Pending             0          16s
```