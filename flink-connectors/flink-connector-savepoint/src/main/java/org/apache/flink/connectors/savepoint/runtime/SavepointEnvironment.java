/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.savepoint.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.util.ExceptionUtils;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * A minimally implemented {@link Environment} that provides
 * the functionality required to run the {@code savepoint-connector}.
 */
@Internal
public class SavepointEnvironment implements Environment {
	private static final String MSG = "This method should never be called";

	private final RuntimeContext ctx;

	private final Configuration configuration;

	private final int maxParallelism;

	private final IOManager ioManager;

	public SavepointEnvironment(RuntimeContext ctx, Configuration configuration, int maxParallelism) {
		this.ctx = ctx;
		this.configuration = configuration;
		this.maxParallelism = maxParallelism;
		this.ioManager = new IOManagerAsync();
	}

	@Override
	public ExecutionConfig getExecutionConfig() {
		return ctx.getExecutionConfig();
	}

	@Override
	public JobID getJobID() {
		return new JobID(0, 0);
	}

	@Override
	public JobVertexID getJobVertexId() {
		return new JobVertexID(0, 0);
	}

	@Override
	public ExecutionAttemptID getExecutionId() {
		return new ExecutionAttemptID(0, 0);
	}

	@Override
	public Configuration getTaskConfiguration() {
		return configuration;
	}

	@Override
	public TaskManagerRuntimeInfo getTaskManagerInfo() {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public TaskMetricGroup getMetricGroup() {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public Configuration getJobConfiguration() {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public TaskInfo getTaskInfo() {
		return new TaskInfo(
			ctx.getTaskName(),
			maxParallelism,
			ctx.getIndexOfThisSubtask(),
			ctx.getNumberOfParallelSubtasks(),
			ctx.getAttemptNumber());
	}

	@Override
	public InputSplitProvider getInputSplitProvider() {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public IOManager getIOManager() {
		return ioManager;
	}

	@Override
	public MemoryManager getMemoryManager() {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public ClassLoader getUserClassLoader() {
		return ctx.getUserCodeClassLoader();
	}

	@Override
	public Map<String, Future<Path>> getDistributedCacheEntries() {
		return Collections.emptyMap();
	}

	@Override
	public BroadcastVariableManager getBroadcastVariableManager() {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public TaskStateManager getTaskStateManager() {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public AccumulatorRegistry getAccumulatorRegistry() {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public TaskKvStateRegistry getTaskKvStateRegistry() {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics) {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics, TaskStateSnapshot subtaskState) {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public void declineCheckpoint(long checkpointId, Throwable cause) {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public void failExternally(Throwable cause) {
		ExceptionUtils.rethrow(cause);
	}

	@Override
	public ResultPartitionWriter getWriter(int index) {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public ResultPartitionWriter[] getAllWriters() {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public InputGate getInputGate(int index) {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public InputGate[] getAllInputGates() {
		throw new UnsupportedOperationException(MSG);
	}

	@Override
	public TaskEventDispatcher getTaskEventDispatcher() {
		throw new UnsupportedOperationException(MSG);
	}
}
