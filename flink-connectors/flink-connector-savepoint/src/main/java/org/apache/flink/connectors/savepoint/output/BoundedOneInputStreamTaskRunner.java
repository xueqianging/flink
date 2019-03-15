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

package org.apache.flink.connectors.savepoint.output;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.savepoint.runtime.SavepointEnvironment;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * A {@link RichMapPartitionFunction} that serves as the runtime for a {@link
 * BoundedOneInputStreamTask}.
 *
 * <p>The task is executed processing the data in a particular partition instead of the pulling from
 * the network stack. After all data has been processed the runner will output the {@link
 * OperatorSubtaskState} from the snapshot of the bounded task.
 *
 * @param <IN> Type of the input to the partition
 */
@Internal
public class BoundedOneInputStreamTaskRunner<IN> extends RichMapPartitionFunction<IN, Tuple2<Integer, OperatorSubtaskState>> {
	private final StreamConfig streamConfig;

	@Nullable
	private final TimestampAssigner<IN> timestampAssigner;

	private final int maxParallelism;

	private final Path savepointPath;

	/**
	 * Create a new {@link BoundedOneInputStreamTaskRunner}.
	 *
	 * @param streamConfig The internal configuration for the task.
	 * @param timestampAssigner An optional timestamp assigner for the records.
	 * @param maxParallelism The max parallelism of the operator. Equivalent to setting {@link
	 *     org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#setMaxParallelism(int)}.
	 * @param savepointPath The directory where the savepoint should be written.
	 */
	public BoundedOneInputStreamTaskRunner(StreamConfig streamConfig, @Nullable TimestampAssigner<IN> timestampAssigner, int maxParallelism, Path savepointPath) {
		this.streamConfig = streamConfig;
		this.timestampAssigner = timestampAssigner;
		this.maxParallelism = maxParallelism;
		this.savepointPath = savepointPath;
	}

	@Override
	public void mapPartition(Iterable<IN> values, Collector<Tuple2<Integer, OperatorSubtaskState>> out) throws Exception {
		SavepointEnvironment env = new SavepointEnvironment(getRuntimeContext(), streamConfig.getConfiguration(), maxParallelism);
		BoundedOneInputStreamTask<IN, ?> task = new BoundedOneInputStreamTask<>(
			env,
			savepointPath,
			values,
			timestampAssigner);

		task.invoke();

		int index = getRuntimeContext().getIndexOfThisSubtask();
		OperatorSubtaskState subtaskState = task.getState();

		out.collect(Tuple2.of(index, subtaskState));
	}
}

