---
title: "Setup Task Executor Memory"
nav-parent_id: ops_mem
nav-pos: 1
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

Apache Flink provides efficient workloads on top of the JVM by tightly controlling the memory usage of its various components.
While the community strives to offer sensible defaults to all configurations, the full breadth of applications
that users deploy on Flink means this isn't always possible. To provide the most production value to our users,
Flink allows both high level and fine-grained tuning of memory allocation within clusters.

* toc
{:toc}

The further described memory configuration is applicable starting with the release version 1.10.

<strong>Note: This memory setup guide is relevant only for Task Executors!</strong>
Check Job Manager [related configuration options](../config.html#jobmanager) for the memory setup of Job Manager.

## Configure Total Memory

The *total process memory* of Flink JVM processes consists of memory consumed by Flink application (*total Flink memory*)
and by the JVM to run the process. The *total Flink memory* consumption includes usage of JVM heap,
*managed memory* (managed by Flink) and other direct (or native) memory.

<center>
  <img src="{{ site.baseurl }}/fig/simple_mem_model.svg" width="300px" alt="Simple memory model" usemap="#simple-mem-model">
</center>
<br />

If you run FIink locally (e.g. from your IDE) without creating a cluster, then only a subset of the memory configuration
options are relevant, see also [local execution](#local-execution) for more details.

Otherwise, the simplest way to setup memory in Flink is to configure either of the two following options:
* Total Flink memory ([taskmanager.memory.flink.size](../config.html#taskmanager-memory-flink-size-1))
* Total process memory ([taskmanager.memory.process.size](../config.html#taskmanager-memory-process-size-1))

The rest of the memory components will be adjusted automatically, based on default values or additionally configured options.
[Here](#detailed-memory-model) are more details about the other memory components.

Configuring *total Flink memory* is better suited for standalone deployments where you want to declare how much memory
is given to Flink itself. The *total Flink memory* splits up into [managed memory size](#managed-memory) and JVM heap.

If you configure *total process memory* you declare how much memory in total should be assigned to the Flink *JVM process*.
For the containerized deployments it corresponds to the size of the requested container, see also
[how to configure memory for containers](#heading=h.q0nx4u2c3pzx)
([Kubernetes](../deployment/kubernetes.html), [Yarn](../deployment/yarn_setup.html) or [Mesos](../deployment/mesos.html)).

Another way to setup the memory is to set [task heap](#task-operator-heap-memory) and [managed memory](#managed-memory)
([taskmanager.memory.task.heap.size](../config.html#taskmanager-memory-task-heap-size) and [taskmanager.memory.managed.size](../config.html#taskmanager-memory-managed-size)).
This more fine-grained approach is described further in a [separate section](#configure-heap-and-managed-memory).

<strong>Note:</strong> One of the three mentioned ways has to be used to configure Flink’s memory (except for local execution), or the Flink startup will fail.
This means that one of the following option subsets, which do not have default values, have to be configured explicitly in *flink-conf.yaml*:
* [taskmanager.memory.flink.size](../config.html#taskmanager-memory-flink-size-1)
* [taskmanager.memory.process.size](../config.html#taskmanager-memory-process-size-1)
* [taskmanager.memory.task.heap.size](../config.html#taskmanager-memory-task-heap-size) and [taskmanager.memory.managed.size](../config.html#taskmanager-memory-managed-size)

<strong>Note:</strong> Explicitly configuring both *total process memory* and *total Flink memory* is not recommended.
It may lead to deployment failures due to potential memory configuration conflicts. Additional configuration
of other memory components also requires caution as it can produce further configuration conflicts.
The conflict can occur e.g. if the sum of sub-components does not add up to the total configured memory or size of some
component is outside of its min/max range, see also [the detailed memory model](#detailed-memory-model).

## Configure Heap and Managed Memory

As mentioned before in [total memory description](#configure-total-memory), another way to setup memory in Flink is
to specify explicitly both [task heap](#task-operator-heap-memory) and [managed memory](#managed-memory).
It gives more control over the available JVM heap to Flink’s tasks and its [managed memory](#managed-memory).

The rest of the memory components will be adjusted automatically, based on default values or additionally configured options.
[Here](#detailed-memory-model) are more details about the other memory components.

<strong>Note:</strong> If you have explicitly configured task heap and managed memory, it is recommended to set neither
*total process memory* nor *total Flink memory*. Otherwise, it may lead to deployment failures due to potential memory configuration conflicts.

### Task (Operator) Heap Memory

If you want to guarantee that a certain amount of JVM heap is available for your user code, you can set the *task heap memory*
explicitly ([taskmanager.memory.task.heap.size](../config.html#taskmanager-memory-task-heap-size)).
It will be added to the JVM heap size and will be dedicated to Flink’s operators running the user code.

### Managed memory

This type of memory is native (off-heap) and explicitly managed by Flink. Use cases for *managed memory* are the following:
* Streaming jobs can use it for [RocksDB state backend](../state/state_backends.html#the-rocksdbstatebackend).
* [Batch jobs](../../dev/batch) can use it for sorting, hash tables, caching of intermediate results.

The size of managed memory can be
* either configured explicitly ([taskmanager.memory.managed.size](../config.html#taskmanager-memory-managed-size))
* or computed as a fraction of *total Flink memory* ([taskmanager.memory.managed.fraction](../config.html#taskmanager-memory-managed-fraction))

If both *size* and *fraction* are explicitly set, the *size* will be used and the *fraction* will be ignored.
If neither *size* nor *fraction* is explicitly configured, the [default fraction](../config.html#taskmanager-memory-managed-fraction) will be used.

See also [how to configure memory for state backends](#heading=h.srelwz7nbzwa) and [batch jobs](#heading=h.d6mjc9yd85c0).

## Configure off-heap memory (direct or native)

The off-heap memory, allocated in user code, should be accounted for in *task off-heap memory*
([taskmanager.memory.task.off-heap.size](../config.html#taskmanager-memory-task-off-heap-size)).

<strong>Note:</strong> You can also increase [framework off-heap memory](#framework-memory) but this option is advanced
and recommended to be changed if you are sure that Flink framework needs more memory.

Flink includes the *framework off-heap memory* and *task off-heap memory* into the *direct memory* limit controlled by JVM,
see also [JVM Parameters](#jvm-parameters).

<strong>Note:</strong> Although, the native non-direct usage of memory can be accounted for as a part of the
*framework off-heap memory* or *task off-heap memory*, it means that the actual *direct memory* allocation will have
a bigger JVM limit in this case.

<strong>Note:</strong> The *network memory* is also part of JVM *direct memory* but it is managed by Flink and guaranteed
that it is always allocated within its configured size. Therefore, resizing network memory will not necessarily help in this situation.

See also [detailed Memory Model](#detailed-memory-model).

## Detailed Memory Model

This section gives a detailed description of all components in Flink’s memory model.

<br />
<center>
  <img src="{{ site.baseurl }}/fig/detailed-mem-model.svg" width="300px" alt="Simple memory model" usemap="#simple-mem-model">
</center>
<br />
\* notice, that the *native non-direct* usage of memory in user code can be also accounted for as a part of the *task off-heap memory*,
see also [how to configure off-heap memory](#configure-off-heap-memory-direct-or-native)

The following table lists all memory components, depicted above, and references Flink configuration options
which affect the size of the respective components:

| **Component**                                  | **Configuration options**                                                                                                                                                                                                                                                             | **Description**                                                                                                                                                                                                                                 |
| :----------------------------------------------| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Framework Heap Memory](#framework-memory)     | [taskmanager.memory.framework.heap.size](../config.html#taskmanager-memory-framework-heap-size)                                                                                                                                                                                       | JVM heap memory dedicated to Flink framework (advanced option)                                                                                                                                                                                  |
| [Task Heap Memory](#task-operator-heap-memory) | [taskmanager.memory.task.heap.size](../config.html#taskmanager-memory-task-heap-size)                                                                                                                                                                                                 | JVM heap memory dedicated to Flink application to run operators and user code                                                                                                                                                                   |
| [Managed memory](#managed-memory)              | [taskmanager.memory.managed.size](../config.html#taskmanager-memory-managed-size) <br/> [taskmanager.memory.managed.fraction](../config.html#taskmanager-memory-managed-fraction)                                                                                                     | Native memory managed by Flink, reserved for sorting, hash tables, caching of intermediate results and RocksDB state backend                                                                                                                    |
| [Framework Off-heap Memory](#framework-memory) | [taskmanager.memory.framework.off-heap.size](../config.html#taskmanager-memory-framework-off-heap-size)                                                                                                                                                                               | [Off-heap direct (or native) memory](#configure-off-heap-memory-direct-or-native) dedicated to Flink framework (advanced option))                                                                                                               |
| Task Off-heap Memory                           | [taskmanager.memory.task.off-heap.size](../config.html#taskmanager-memory-task-off-heap-size)                                                                                                                                                                                         | [Off-heap direct (or native) memory](#configure-off-heap-memory-direct-or-native) dedicated to Flink application to run operators                                                                                                               |
| Network Memory                                 | [taskmanager.memory.network.min](../config.html#taskmanager-memory-network-min) <br/> [taskmanager.memory.network.max](../config.html#taskmanager-memory-network-max) <br/> [taskmanager.memory.network.fraction](../config.html#taskmanager-memory-network-fraction)                 | Direct memory reserved for data record exchange between tasks (e.g. buffering for the transfer over the network), it is a [capped fractionated component](#capped-fractionated-components) of the [total Flink memory](#configure-total-memory) |
| [JVM metaspace](#jvm-parameters)               | [taskmanager.memory.jvm-metaspace.size](../config.html#taskmanager-memory-jvm-metaspace-size)                                                                                                                                                                                         | Metaspace size of the Flink JVM process                                                                                                                                                                                                         |
| JVM Overhead                                   | [taskmanager.memory.network.min](../config.html#taskmanager-memory-jvm-overhead-min) <br/> [taskmanager.memory.network.max](../config.html#taskmanager-jvm-overhead-network-max) <br/> [taskmanager.memory.network.fraction](../config.html#taskmanager-memory-jvm-overhead-fraction) | Native memory reserved for other JVM overhead: e.g. thread stacks, code cache, garbage collection space etc, it is a [capped fractionated component](#capped-fractionated-components) of the [total process memory](#configure-total-memory)    |

As you can see, the size of some memory components can be simply set by the respective option.
Other components can be tuned using multiple options.

### Framework memory

The *framework heap memory* and *framework off-heap memory* options are not supposed to be changed without a good reason.
Adjust them only if you are sure that Flink needs more memory for some internal data structures or operations.
It can be related to a particular deployment environment or job structure, like high parallelism.
In addition, Flink dependencies, such as Hadoop may consume more direct or native memory in certain setups.

<strong>Note:</strong> Neither heap nor off-heap versions of framework and task memory are currently isolated within Flink.
The separation of framework and task memory can be used in future releases for further optimizations.

### Capped fractionated components

This section describes the configuration details of the following options which can be a fraction of a certain
[total memory](#configure-total-memory):

* *Network memory* can be a fraction of the *total Flink memory*
* *JVM overhead* can be a fraction of the *total process memory*

See also [detailed memory model](#detailed-memory-model).

The size of those components always has to be between its maximum and minimum value, otherwise Flink startup will fail.
The maximum and minimum values have defaults or can be explicitly set by corresponding configuration options.
Notice if you configure the same maximum and minimum value it effectively means that its size is fixed to that value.
For example, if the following options are:
- total Flink memory = 1000mb,
- network min = 64mb,
- network max = 128mb,
- network fraction = 0.1

then the network memory will be 1000mb x 0.1 = 100Mb which is within the range 64-128Mb.

If the component memory is not explicitly configured, then Flink will use the fraction to calculate the memory size
based on the total memory. The calculated value is capped by its corresponding min/max options.
For example, if the following options are:
- total Flink memory = 1000mb,
- network min = 128mb,
- network max = 256mb,
- network fraction = 0.1

then the network memory will be 128Mb because the fraction is 100mb and it is less than the minimum.

It can also happen that the fraction is ignored if the size of the total memory and its other components is defined.
In this case, the network memory is the rest of the total memory. The derived value still has to be within its min/max
range otherwise the configuration fails. For example, suppose the following options are set:
- total Flink memory = 1000mb,
- task heap = 100mb,
- network min = 64mb,
- network max = 256mb,
- network fraction = 0.1

All other components of the total Flink memory have default values, including the default managed memory fraction.
Then the network memory is not the fraction (1000mb x 0.1 = 100Mb) but the rest of the total Flink memory
which will either be within the range 64-256Mb or fail.

## JVM Parameters

Flink explicitly adds the following memory related JVM arguments while starting the task executor process,
based on the configured or derived memory component sizes:

| **JVM Arguments**         | **Value**                                  |
| :------------------------ | :----------------------------------------- |
| *-Xmx* and *-Xms*         | Framework + Task Heap Memory               |
| *-XX:MaxDirectMemorySize* | Framework + Task Off-Heap + Network Memory |
| *-XX:MaxMetaspaceSize*    | JVM Metaspace                              |

See also [detailed memory model](#detailed-memory-model).

## Local Execution
If you start Flink locally on your machine as a single java program without creating a cluster (e.g. from your IDE)
then all components are ignored except for the following:

| **Memory component**     | **Relevant options**                                                                          | **Default value for the local execution**                                     |
| :----------------------- | :-------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------- |
| Task heap                | [taskmanager.memory.task.heap.size](../config.html#taskmanager-memory-task-heap-size)         | infinite                                                                      |
| Task off-heap            | [taskmanager.memory.task.off-heap.size](../config.html#taskmanager-memory-task-off-heap-size) | infinite                                                                      |
| Managed memory           | [taskmanager.memory.managed.size](../config.html#taskmanager-memory-managed-size)             | 128Mb                                                                         |
| Network memory           | [taskmanager.memory.network.min](../config.html#taskmanager-memory-network-min) <br /> [taskmanager.memory.network.max](../config.html#taskmanager-memory-network-max) | 64Mb |

All of the components listed above can be but do not have to be explicitly configured for the local execution.
If they are not configured they are set to their default values. [Task heap memory](#task-operator-heap-memory) and
*task off-heap memory* is considered to be infinite (*Long.MAX_VALUE* bytes) and [managed memory](#managed-memory)
has a default value of 128Mb only for the local execution mode.

<strong>Note:</strong> The task heap size is not related in any way to the real heap size in this case.
It can become relevant for future optimizations coming with next releases. The actual JVM heap size of the started
local process is not controlled by Flink and depends on how you start the process.
If you want to control the JVM heap size you have to explicitly pass the corresponding JVM arguments, e.g. *-Xmx*, *-Xms*.
