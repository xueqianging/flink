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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.savepoint.output.BoundedOneInputStreamTaskRunner;
import org.apache.flink.connectors.savepoint.output.partitioner.HashSelector;
import org.apache.flink.connectors.savepoint.output.partitioner.KeyGroupRangePartitioner;
import org.apache.flink.connectors.savepoint.runtime.OperatorIDGenerator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.graph.StreamConfig;

import javax.annotation.Nullable;

/**
 * An Operator represents a single operator within a {@link Savepoint}.
 */
public abstract class Operator {

	/**
	 * Create a new {@link Operator} from a {@link DataSet}.
	 *
	 * @param dataSet A dataset of elements.
	 * @param <T> The type of the input.
	 * @return A {@link OneInputOperator}.
	 */
	public static <T> OneInputOperator<T> fromDataSet(DataSet<T> dataSet) {
		return new OneInputOperator<>(dataSet);
	}

	private Operator() {}

	abstract DataSet<Tuple2<Integer, OperatorSubtaskState>> getOperatorSubtaskStates(
		String uid,
		StateBackend stateBackend,
		int maxParallelism,
		Path savepointPath);

	static class OneInput<T> extends Operator {

		private final DataSet<T> dataSet;

		private final StreamConfig config;

		@Nullable private final HashSelector<T> keySelector;

		@Nullable private final TimestampAssigner<T> timestampAssigner;

		OneInput(DataSet<T> dataSet, StreamConfig config, @Nullable TimestampAssigner<T> timestampAssigner) {
			this.dataSet = dataSet;
			this.config = config;
			this.keySelector = null;
			this.timestampAssigner = timestampAssigner;
		}

		<K> OneInput(
			DataSet<T> dataSet,
			StreamConfig config,
			KeySelector<T, K> keySelector,
			@Nullable TimestampAssigner<T> timestampAssigner) {
			this.dataSet = dataSet;
			this.config = config;
			this.keySelector = new HashSelector<>(keySelector);
			this.timestampAssigner = timestampAssigner;
		}

		@Override
		DataSet<Tuple2<Integer, OperatorSubtaskState>> getOperatorSubtaskStates(
			String uid,
			StateBackend stateBackend,
			int maxParallelism,
			Path savepointPath) {
			DataSet<T> input = dataSet;
			if (keySelector != null) {
				input = dataSet.partitionCustom(new KeyGroupRangePartitioner(maxParallelism), keySelector);
			}

			config.setOperatorName(uid);
			config.setOperatorID(OperatorIDGenerator.fromUid(uid));
			config.setStateBackend(stateBackend);

			BoundedOneInputStreamTaskRunner<T> operatorRunner =
				new BoundedOneInputStreamTaskRunner<>(
					config, timestampAssigner, maxParallelism, savepointPath);

			return input.mapPartition(operatorRunner);
		}
	}
}

