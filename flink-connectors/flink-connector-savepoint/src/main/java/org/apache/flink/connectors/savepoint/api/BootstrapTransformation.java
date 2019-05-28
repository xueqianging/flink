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

package org.apache.flink.connectors.savepoint.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.connectors.savepoint.operators.BroadcastStateBootstrapOperator;
import org.apache.flink.connectors.savepoint.output.BoundedOneInputStreamTaskRunner;
import org.apache.flink.connectors.savepoint.output.TaggedOperatorSubtaskState;
import org.apache.flink.connectors.savepoint.output.metadata.SavepointMetadataProvider;
import org.apache.flink.connectors.savepoint.output.partitioner.HashSelector;
import org.apache.flink.connectors.savepoint.output.partitioner.KeyGroupRangePartitioner;
import org.apache.flink.connectors.savepoint.runtime.BoundedStreamConfig;
import org.apache.flink.connectors.savepoint.runtime.OperatorIDGenerator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamOperator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Bootstrapped data that can be written into a {@code Savepoint}.
 * @param <T> The input type of the transformation.
 */
@SuppressWarnings("WeakerAccess")
@PublicEvolving
public class BootstrapTransformation<T> {
	private final DataSet<T> dataSet;

	private final SavepointWriterOperatorFactory factory;

	@Nullable
	private final HashSelector<T> keySelector;

	@Nullable
	private final TypeInformation<?> keyType;

	BootstrapTransformation(
		DataSet<T> dataSet,
		SavepointWriterOperatorFactory factory) {
		this.dataSet = dataSet;
		this.factory = factory;
		this.keySelector = null;
		this.keyType = null;
	}

	<K> BootstrapTransformation(
		DataSet<T> dataSet,
		SavepointWriterOperatorFactory factory,
		@Nonnull KeySelector<T, K> keySelector,
		@Nonnull TypeInformation<K> keyType) {
		this.dataSet = dataSet;
		this.factory = factory;
		this.keySelector = new HashSelector<>(keySelector);
		this.keyType = keyType;
	}

	DataSet<TaggedOperatorSubtaskState> getOperatorSubtaskStates(
		String uid,
		StateBackend stateBackend,
		SavepointMetadataProvider provider,
		Path savepointPath) {
		DataSet<T> input = dataSet;
		if (keySelector != null) {
			input = dataSet.partitionCustom(new KeyGroupRangePartitioner(provider), keySelector);
		}

		final StreamConfig config;
		if (keyType == null) {
			config = new BoundedStreamConfig();
		} else {
			TypeSerializer<?> keySerializer = keyType.createSerializer(dataSet.getExecutionEnvironment().getConfig());
			config = new BoundedStreamConfig(keySerializer, keySelector);
		}

		StreamOperator<TaggedOperatorSubtaskState> operator = factory.getOperator(
			System.currentTimeMillis(),
			savepointPath);

		operator = dataSet.clean(operator);
		config.setStreamOperator(operator);

		config.setOperatorName(uid);
		config.setOperatorID(OperatorIDGenerator.fromUid(uid));
		config.setStateBackend(stateBackend);

		BoundedOneInputStreamTaskRunner<T> operatorRunner = new BoundedOneInputStreamTaskRunner<>(
			config,
			provider
		);

		MapPartitionOperator<T, TaggedOperatorSubtaskState> subtaskStates = input
			.mapPartition(operatorRunner)
			.name(uid);

		if (config.getStreamOperator(getClass().getClassLoader()) instanceof BroadcastStateBootstrapOperator) {
			subtaskStates = subtaskStates.setParallelism(1);
		}

		return subtaskStates;
	}
}
