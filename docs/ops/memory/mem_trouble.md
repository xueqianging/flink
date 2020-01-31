---
title: "Troubleshooting"
nav-parent_id: ops_mem
nav-pos: 3
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

## IllegalConfigurationException

If you see an *IllegalConfigurationException* thrown from *TaskExecutorProcessUtils*, it usually indicates
that there is either an invalid configuration value (e.g., negative memory size, fraction that is greater than 1, etc.)
or configuration conflicts. Check the documentation chapters related to the [memory components](mem_setup.html#detailed-memory-model)
mentioned in the exception message.

## OutOfMemoryError: Java heap space

The exception usually indicates that JVM heap is not configured with enough size. You can try to increase the JVM heap size
by configuring larger [total memory](mem_setup.html#configure-total-memory) or [task heap memory](mem_setup.html#task-operator-heap-memory).

<strong>Note:</strong> You can also increase [framework heap memory](mem_setup.html#framework-memory) but this option
is advanced and recommended to be changed if you are sure that the Flink framework itself needs more memory.

## OutOfMemoryError: Direct buffer memory

The exception usually indicates that the JVM *direct memory* limit is too small if there is no *direct memory leak*.
You can try to increase this limit by adjusting [direct off-heap memory](mem_setup.html#detailed-memory-model).
See also [how to configure off-heap memory](mem_setup.html#configure-off-heap-memory-direct-or-native) and
[JVM arguments](mem_setup.html#jvm-parameters) which Flink sets.

## OutOfMemoryError: Metaspace

The exception usually indicates that [JVM metaspace](mem_setup.html#jvm-parameters) limit is configured too small.
You can try to increase the [JVM metaspace](../config.html#taskmanager-memory-jvm-metaspace-size).

## IOException: Insufficient number of network buffers

The exception usually indicates that the size of the configured [network memory](mem_setup.html#detailed-memory-model)
is not big enough.

## Container Memory exceeded

If a task executor container tries to allocate memory beyond its requested size (Yarn, Mesos or Kubernetes),
this usually indicates that Flink has not reserved enough native memory. You can observe that either from an external
monitoring system or from the error messages when a container gets killed by deployment environment.

If [RocksDBStateBackend](../state/state_backends.html#the-rocksdbstatebackend) is used and the memory controlling is disabled,
you can try to increase the [managed memory](mem_setup.html#managed-memory).

Alternatively, you can increase the [JVM overhead](mem_setup.html#detailed-memory-model).
See also [how to configure memory for containers](mem_tuning.html#configure-memory-for-containers).
