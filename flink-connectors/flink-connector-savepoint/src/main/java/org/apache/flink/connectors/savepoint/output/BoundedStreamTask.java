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

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.Preconditions;

/**
 * A stream task that pulls elements from an {@link Iterable} instead of the network. After all
 * elements are processed the task takes a snapshot of the subtask operator state.
 *
 * @param <IN> Type of the input.
 * @param <OUT> Type of the output.
 * @param <OP> Type of the operator this task runs.
 */
abstract class BoundedStreamTask<IN, OUT, OP extends StreamOperator<OUT>> extends StreamTask<OUT, OP> {
	private final Iterable<IN> input;

	private final CheckpointMetaData metaData;

	private final CheckpointOptions options;

	private OperatorSubtaskState state;

	private volatile boolean running;

	BoundedStreamTask(
		Environment environment,
		ProcessingTimeService timeProvider,
		Iterable<IN> input,
		Path savepointPath) {
		super(environment, timeProvider);
		this.input = input;
		this.metaData = new CheckpointMetaData(0L, System.currentTimeMillis());
		this.options = new CheckpointOptions(
				CheckpointType.SAVEPOINT,
				AbstractFsCheckpointStorage.encodePathAsReference(savepointPath));
	}

	private void snapshot() throws Exception {
		CheckpointStorage checkpointStorage = stateBackend.createCheckpointStorage(getEnvironment().getJobID());
		headOperator.prepareSnapshotPreBarrier(0);

		CheckpointStreamFactory storage = checkpointStorage.resolveCheckpointStorageLocation(
			metaData.getCheckpointId(),
			options.getTargetLocation());

		headOperator.prepareSnapshotPreBarrier(0L);

		OperatorSnapshotFutures snapshotInProgress = headOperator.snapshotState(
				metaData.getCheckpointId(),
				metaData.getTimestamp(),
				options,
				storage);

		state = new OperatorSnapshotFinalizer(snapshotInProgress).getJobManagerOwnedState();

		headOperator.notifyCheckpointComplete(0);
	}

	protected abstract void process(IN value) throws Exception;

	OperatorSubtaskState getState() {
		return state;
	}

	@Override
	protected void init() {
		Preconditions.checkState(
			operatorChain.getAllOperators().length == 1,
			"BoundedStreamTask's should only run a single operator");

		running = true;
	}

	@Override
	protected void run() throws Exception {
		for (IN value : input) {
			if (!running) {
				break;
			}

			process(value);
		}

		snapshot();
	}

	@Override
	protected void cancelTask() {
		running = false;
	}

	@Override
	protected void cleanup() {}
}

