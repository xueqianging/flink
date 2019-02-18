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

package org.apache.flink.connectors.savepoint.input;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connectors.savepoint.input.splits.OperatorStateInputSplit;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.streaming.api.operators.BackendRestorerProcedure;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Base input format for reading operator state's from a {@link org.apache.flink.runtime.checkpoint.savepoint.Savepoint}.
 *
 * @param <O> The generic type of the state.
 */
abstract class OperatorStateInputFormat<O> extends SavepointInputFormat<O, OperatorStateInputSplit> {

	private transient OperatorStateBackend restoredBackend;

	private transient CloseableRegistry registry;

	private transient Iterator<O> elements;

	OperatorStateInputFormat(String savepointPath, String uid) {
		super(savepointPath, uid);
	}

	protected abstract Iterable<O> getElements(OperatorStateBackend restoredBackend) throws Exception;

	@Override
	public OperatorStateInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		final OperatorState operatorState = getOperatorState();

		createStatistics(operatorState, subtask -> subtask.getManagedOperatorState().getStateSize());

		final Map<OperatorInstanceID, List<OperatorStateHandle>> newManagedOperatorStates = new HashMap<>();
		final Map<OperatorInstanceID, List<OperatorStateHandle>> newRawOperatorStates = new HashMap<>();

		StateAssignmentOperation.reDistributePartitionableStates(
			Collections.singletonList(operatorState),
			minNumSplits,
			Collections.singletonList(getOperatorID()),
			newManagedOperatorStates,
			newRawOperatorStates
		);

		int index = 0;
		final OperatorStateInputSplit[] splits = new OperatorStateInputSplit[newManagedOperatorStates.size()];

		for (final List<OperatorStateHandle> operatorStateHandles : newManagedOperatorStates.values()) {
			OperatorStateInputSplit split = new OperatorStateInputSplit(new StateObjectCollection<>(operatorStateHandles), index);
			splits[index++] = split;
		}

		return splits;
	}

	@Override
	public void open(OperatorStateInputSplit split) throws IOException {
		registry = new CloseableRegistry();

		final BackendRestorerProcedure<OperatorStateBackend, OperatorStateHandle> backendRestorer =
			new BackendRestorerProcedure<>(
				() -> createOperatorStateBackend(getRuntimeContext()),
				registry,
				getUid()
			);

		try {
			restoredBackend = backendRestorer.createAndRestore(split.getPrioritizedManagedOperatorState());
		} catch (Exception exception) {
			throw new IOException("Failed to restore state backend", exception);
		}

		try {
			elements = getElements(restoredBackend).iterator();
		} catch (Exception e) {
			throw new IOException("Failed to read operator state from restored state backend", e);
		}
	}

	@Override
	public void close() {
		registry.unregisterCloseable(restoredBackend);
		IOUtils.closeQuietly(restoredBackend);
		IOUtils.closeQuietly(registry);
	}

	@Override
	public boolean reachedEnd() {
		return !elements.hasNext();
	}

	@Override
	public O nextRecord(O reuse) {
		return elements.next();
	}

	private static OperatorStateBackend createOperatorStateBackend(RuntimeContext runtimeContext) {
		return new DefaultOperatorStateBackend(
			runtimeContext.getUserCodeClassLoader(),
			runtimeContext.getExecutionConfig(),
			false
		);
	}
}

