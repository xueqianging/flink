---
title: "Memory tuning guide"
nav-parent_id: ops_mem
nav-pos: 2
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

* toc
{:toc}

In addition to the [main memory setup guide](mem_setup.html), this section explains how to setup memory of task executors
depending on the use case and which options are important in which case.

## Configure memory for standalone deployment

It is recommended to configure [total Flink memory](mem_setup.html#configure-total-memory)
([taskmanager.memory.flink.size](../config.html#taskmanager-memory-flink-size-1)) or its [components](mem_setup.html#detailed-memory-model)
for [standalone deployment](../deployment/cluster_setup.html) where you want to declare how much memory is given to Flink itself.
Additionally, you can adjust *JVM metaspace* if it causes [problems](mem_trouble.html#outofmemoryerror-metaspace).

The *total Process memory* is not relevant because *JVM overhead* is not controlled by Flink or deployment environment,
only physical resources of the executing machine matter in this case.

## Configure memory for containers

It is recommended to configure [total process memory](mem_setup.html#configure-total-memory)
([taskmanager.memory.process.size](../config.html#taskmanager-memory-process-size-1)) for the containerized deployments
([Kubernetes](../deployment/kubernetes.html), [Yarn](../deployment/yarn_setup.html) or [Mesos](../deployment/mesos.html)).
It declares how much memory in total should be assigned to the Flink *JVM process* and corresponds to the size of the requested container.

<strong>Note:</strong> If you configure the *total Flink memory* Flink will implicitly add JVM memory components
to derive the *total process memory* and request a container with the memory of that derived size,
see also [detailed Memory Model](mem_setup.html#detailed-memory-model).

<div class="alert alert-warning">
  <strong>Warning:</strong> If Flink or user code allocates unmanaged off-heap (native) memory beyond the container size
  the job can fail because the deployment environment can kill the offending containers.
</div>
See also description of [container memory exceeded](mem_trouble.html#container-memory-exceeded) failure.

## Configure memory for state backends

When deploying a Flink streaming application, the type of [state backend](../state/state_backends.html) used
will dictate the optimal memory configurations of your cluster.

### Heap state backend

When running a stateless job or using a heap state backend ([MemoryStateBackend](../state/state_backends.html#the-memorystatebackend)
or [FsStateBackend](../state/state_backends.html#the-fsstatebackend), set [managed memory](mem_setup.html#managed-memory) to zero.
This will ensure that the maximum amount of memory is allocated for user code on the JVM.

### RocksDB state backend

The [RocksDBStateBackend](../state/state_backends.html#the-rocksdbstatebackend) uses native memory. By default,
the RocksDB is setup to limit native memory allocation to the size of the [managed memory](mem_setup.html#managed-memory).
Therefore, it is important to reserve enough *managed memory* for your state use case. If you disable the default RocksDB memory control,
task executors can be killed in containerized deployments if RocksDB allocates memory above the limit of the requested container size
(the [total process memory](mem_setup.html#configure-total-memory)).
See also [state.backend.rocksdb.memory.managed](../config.html#state-backend-rocksdb-memory-managed).

## Configure memory for batch jobs

[Managed memory](../memory/mem_setup.html#managed-memory) helps Flink to run the batch operators efficiently.
Therefore, the size of the [managed memory](mem_setup.html#managed-memory) can affect the performance of [batch jobs](../../dev/batch).
If it makes sense, Flink will try to allocate and use as much *managed memory* as configured for batch jobs but not beyond its limit.
It prevents OutOfMemoryExceptions because Flink knows how much memory it can use to execute operations.
If Flink runs out of [managed memory](../memory/mem_setup.html#managed-memory), it utilizes disk space.
Using [managed memory](../memory/mem_setup.html#managed-memory), some operations can be performed directly
on the raw data without having to deserialize the data to convert it into Java objects. All in all,
[managed memory](../memory/mem_setup.html#managed-memory) improves the robustness and speed of the system.
