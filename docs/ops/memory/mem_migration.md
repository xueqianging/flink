---
title: "Migration from old configuration (before 1.10 release)"
nav-parent_id: ops_mem
nav-pos: 4
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

The [memory setup of task managers](mem_setup.html) has changed a lot with the 1.10 release. Many configuration options
were removed or their semantics changed. This guide will help you to understand how to migrate the previous memory configuration to the new one.

<div class="alert alert-warning">
  <strong>Warning:</strong> It is important to review this guide because the legacy and new memory configuration can
  result in different sizes of memory components. If you try to reuse your Flink configuration from the previous versions
  before 1.10, it can result in changes to the behavior, performance or even configuration failures of your application.
</div>

<strong>Note:</strong> The previous memory configuration allows that no memory related options are set at all
as they all have default values. The [new memory configuration](mem_setup.html#configure-total-memory) requires
that at least one subset of the following options is configured explicitly, otherwise the configuration will fail:
* [taskmanager.memory.flink.size](../config.html#taskmanager-memory-flink-size-1)
* [taskmanager.memory.process.size](../config.html#taskmanager-memory-process-size-1)
* [taskmanager.memory.task.heap.size](../config.html#taskmanager-memory-task-heap-size) and [taskmanager.memory.managed.size](../config.html#taskmanager-memory-managed-size)

The [default ‘flink-conf.yaml’](#default-configuration-in-flink-confyaml) shipped with Flink sets [taskmanager.memory.process.size](../config.html#taskmanager-memory-process-size-1)
to make the default memory configuration consistent.

This [spreadsheet](https://docs.google.com/spreadsheets/d/1mJaMkMPfDJJ-w6nMXALYmTc4XxiV30P5U7DzgwLkSoE) can also help
to evaluate and compare the results of the legacy and new memory computations.

## Changes in Configuration Options

This chapter shortly lists all changes in the memory configuration options before the 1.10 release.
It also references other chapters for more details about the migration to the new configuration options.

The following options are completely removed. If they are still used, they will be just ignored.

| **Removed option**             | **Note**                                                                                                                                                                                                                                                                                                |
| :----------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| taskmanager.memory.fraction    | Check the description of the new option [taskmanager.memory.managed.fraction](../config.html#taskmanager-memory-managed-fraction). The new option has different semantics and the value of the deprecated option usually has to be adjusted. See also [how to migrate managed memory](#managed-memory). |
| taskmanager.memory.off-heap    | on-heap managed memory is no longer supported, see also [how to migrate managed memory](#managed-memory)                                                                                                                                                                                                |
| taskmanager.memory.preallocate | pre-allocation is no longer supported and managed memory is always allocated lazily, see also [how to migrate managed memory](#managed-memory)                                                                                                                                                          |

The following options are deprecated but if they are still used they will be interpreted as new options for backwards compatibility:

| **Deprecated option**               | **Interpreted as**                                                                                                                                                                                                                                                                                                        |
| :---------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| taskmanager.heap.size               | - [taskmanager.memory.flink.size](../config.html#taskmanager-memory-flink-size-1) for standalone deployment <br/> - [taskmanager.memory.process.size](../config.html#taskmanager-memory-process-size-1) for containerised deployments <br/> See also [how to migrate total memory](#total-memory-previously-heap-memory). |
| taskmanager.memory.size             | taskmanager.memory.managed.size, see also [how to migrate managed memory](#managed-memory)                                                                                                                                                                                                                                |
| taskmanager.network.memory.min      | [taskmanager.memory.network.min](../config.html#taskmanager-memory-network-min)                                                                                                                                                                                                                                           |
| taskmanager.network.memory.max      | [taskmanager.memory.network.max](../config.html#taskmanager-memory-network-max)                                                                                                                                                                                                                                           |
| taskmanager.network.memory.fraction | [taskmanager.memory.network.fraction](../config.html#taskmanager-memory-network-fraction)                                                                                                                                                                                                                                 |

Although, the network memory configuration has not changed too much it is recommended to verify its configuration.
It can change if other memory components have new sizes, e.g. the total memory which the network can be a fraction of.
See also [new detailed memory model](mem_setup.html#detailed-memory-model).

The container cut-off configuration options, ‘containerized.heap-cutoff-ratio’ and ‘containerized.heap-cutoff-min’,
have no effect for task manager processes anymore but they still have the same semantics for the job manager process.
See also [how to migrate container cut-off](#container-cut-off-memory).

## Total Memory (previously Heap Memory)

The previous options which were responsible for the total memory used by Flink are ‘taskmanager.heap.size’ or ‘taskmanager.heap.mb’.
Despite their naming, they included not only JVM heap but also other off-heap memory components. The options have been deprecated.

The Mesos integration also had a separate option with the same semantics: ‘mesos.resourcemanager.tasks.mem’ which has also been deprecated.

If the mentioned legacy options are used without specifying the corresponding new options,
they will be directly translated into the following new options:
* Total Flink memory ([taskmanager.memory.flink.size](../config.html#taskmanager-memory-flink-size-1)) for standalone deployments
* Total process memory ([taskmanager.memory.process.size](../config.html#taskmanager-memory-process-size-1)) for containerized deployments (Yarn or Mesos)

It is also recommended to use these new options instead of the legacy ones as they might be completely removed in the following releases.

See also [how to configure total memory now](mem_setup.html#configure-total-memory).

## JVM Heap Memory

JVM heap memory previously consisted of the managed memory (if configured to be on-heap) and the rest
which included any other usages of heap memory. This rest was always implicitly derived as the remaining part of the total memory,
see also [how to migrate managed memory](#managed-memory).

Now, if only *total Flink memory* or *total process memory* is configured, then the JVM heap is also derived as the rest of
what is left after subtracting all other components from the total memory, see also [how to configure total memory](mem_setup.html#configure-total-memory).

Additionally, you can now have more direct control over the JVM heap assigned to the operator tasks
([taskmanager.memory.task.heap.size](../config.html#taskmanager-memory-task-heap-size)),
see also [Task (Operator) Heap Memory](mem_setup.html#task-operator-heap-memory).
The same memory has to be accounted for the heap state backend ([MemoryStateBackend](../state/state_backends.html#the-memorystatebackend)
or [FsStateBackend](../state/state_backends.html#the-fsstatebackend) if it is chosen for streaming jobs.

A part of the JVM heap is now always reserved for Flink framework
([taskmanager.memory.framework.heap.size](../config.html#taskmanager-memory-framework-heap-size)).
See also [Framework memory](mem_setup.html#framework-memory).

## Managed Memory

See also [how to configure managed memory now](mem_setup.html#managed-memory).

### Explicit size

The previous option to configure managed memory size (‘taskmanager.memory.size’) was renamed to
[taskmanager.memory.managed.size](../config.html#taskmanager-memory-managed-size) and deprecated.
It is recommended to use the new option because the legacy one can be removed in future releases.

### Fraction

If not set explicitly, the managed memory could be previously specified as a fraction (‘taskmanager.memory.fraction’)
of the total memory minus network memory and container cut-off (only for [Yarn](../deployment/yarn_setup.html) and
[Mesos](../deployment/mesos.html) deployments). This option has been completely removed and will have no effect if still used.
Please, use the new option [taskmanager.memory.managed.fraction](../config.html#taskmanager-memory-managed-fraction) instead.
This new option will set the [managed memory](mem_setup.html#managed-memory) to the specified fraction of the
[total Flink memory](mem_setup.html#configure-total-memory) if its size is not set explicitly by
[taskmanager.memory.managed.size](../config.html#taskmanager-memory-managed-size).

### RocksDB state

If the [RocksDBStateBackend](../state/state_backends.html#the-rocksdbstatebackend) is chosen for a streaming job,
its native memory consumption should now be accounted for in [managed memory](mem_setup.html#managed-memory).
The RocksDB memory allocation is limited by the [managed memory](mem_setup.html#managed-memory) size.
This should prevent the killing of containers on [Yarn](../deployment/yarn_setup.html) or [Mesos](../deployment/mesos.html).
See also [how to migrate container cut-off](#container-cut-off-memory).

### Other changes

Additionally, the following changes have been made:
* The [managed memory](mem_setup.html#managed-memory) is always off-heap now. The configuration option ‘taskmanager.memory.off-heap’ is removed and will have no effect anymore.
* The [managed memory](mem_setup.html#managed-memory) now uses native memory which is not direct memory. It means that the managed memory is no longer accounted for in the JVM direct memory limit.
* The [managed memory](mem_setup.html#managed-memory) is always lazily allocated now. The configuration option ‘taskmanager.memory.preallocate’ is removed and will have no effect anymore.

## Container Cut-Off Memory

For containerized deployments, you could previously specify a cut-off memory. This memory could accommodate for unaccounted memory allocations.
Dependencies which were not directly controlled by Flink were the main source of those allocations, e.g. RocksDB, internals of JVM etc.
This is no longer available and the related configuration options (‘containerized.heap-cutoff-ratio’ and ‘containerized.heap-cutoff-min’)
will have no effect on the task manager process anymore. The new memory model introduced more specific memory components,
described further, to address these concerns.

In streaming jobs which use [RocksDBStateBackend](../state/state_backends.html#the-rocksdbstatebackend), the RocksDB
native memory consumption should be accounted for as a part of the [managed memory](mem_setup.html#managed-memory) now.
The RocksDB memory allocation is also limited by the configured size of the [managed memory](mem_setup.html#managed-memory).
See also [migrating managed memory](#managed-memory) and [how to configure managed memory now](mem_setup.html#managed-memory).

The other specific consumption of direct or native off-heap memory can be now addressed by the following new configuration options:
* Task off-heap memory ([taskmanager.memory.task.off-heap.size](../config.html#taskmanager-memory-task-off-heap-size))
* Framework off-heap memory ([taskmanager.memory.framework.off-heap.size](../config.html#taskmanager-memory-framework-off-heap-size))
* JVM metaspace ([taskmanager.memory.jvm-metaspace.size](../config.html#taskmanager-memory-jvm-metaspace-size))
* JVM overhead (see also [detailed new memory model](mem_setup.html#detailed-memory-model))

<strong>Note:</strong> The job manager still has container cut-off memory configuration options.
The mentioned configuration options remain valid for the job manager in the same way as before.

## Default Configuration in flink-conf.yaml

This section describes the changes of the default ‘flink-conf.yaml’ shipped with Flink.

The total memory (‘taskmanager.heap.size’) is replaced by [taskmanager.memory.process.size](../config.html#taskmanager-memory-process-size-1)
in the default ‘flink-conf.yaml’. The value is also increased from 1024Mb to 1568Mb.
See also [how to configure total memory now](mem_setup.html#configure-total-memory).

<div class="alert alert-warning">
  <strong>Warning:</strong> If you use the new default ‘flink-conf.yaml’ it can result in different sizes of
  the memory components and can lead to performance changes.
</div>
