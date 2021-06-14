/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.sorted.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class BatchExecutionBootstrapTest extends TestLogger {

    @Test
    public void testBootstrap() throws Exception {
        KeyGroupRange range = new KeyGroupRange(0, 9);
        HeapKeyedStateBackend<Integer> persistent =
                new HeapKeyedStateBackendBuilder<>(
                                mock(TaskKvStateRegistry.class),
                                IntSerializer.INSTANCE,
                                getClass().getClassLoader(),
                                range.getNumberOfKeyGroups(),
                                range,
                                new ExecutionConfig(),
                                TtlTimeProvider.DEFAULT,
                                LatencyTrackingStateConfig.disabled(),
                                Collections.emptyList(),
                                UncompressedStreamCompressionDecorator.INSTANCE,
                                TestLocalRecoveryConfig.disabled(),
                                new HeapPriorityQueueSetFactory(
                                        range, range.getNumberOfKeyGroups(), 128),
                                true,
                                new CloseableRegistry())
                        .build();

        CheckpointableKeyedStateBackend<Integer> backend =
                new BatchExecutionKeyedStateBackend<>(IntSerializer.INSTANCE, range, persistent);

        ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

        ValueState<String> state =
                backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

        ValueState<String> fromPersisted =
                persistent.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

        backend.setCurrentKey(1);
        assertNull(state.value());

        state.update("Ciao");
        assertEquals("Ciao", state.value());

        assertNull(fromPersisted.value());

        // toggle keys to force a flush
        backend.setCurrentKey(2);
        backend.setCurrentKey(1);

        assertEquals("Ciao", fromPersisted.value());

        backend.dispose();
    }
}
