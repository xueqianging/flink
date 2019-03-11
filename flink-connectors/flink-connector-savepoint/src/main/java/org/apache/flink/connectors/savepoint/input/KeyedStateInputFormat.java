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

package org.apache.flink.connectors.savepoint.input;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.savepoint.functions.ProcessReaderFunction;
import org.apache.flink.connectors.savepoint.input.splits.KeyGroupRangeInputSplit;
import org.apache.flink.connectors.savepoint.runtime.SavepointEnvironment;
import org.apache.flink.connectors.savepoint.runtime.SavepointRuntimeContext;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.operators.BackendRestorerProcedure;

import org.apache.commons.io.IOUtils;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Input format for reading partitioned state.
 *
 * @param <K> The type of the key.
 * @param <OUT> The type of the output of the {@link ProcessReaderFunction}.
 */
@PublicEvolving
public class KeyedStateInputFormat<K, OUT> extends SavepointInputFormat<OUT, KeyGroupRangeInputSplit> {

	private final StateBackend stateBackend;

	private final TypeInformation<K> keyType;

	private final ProcessReaderFunction<K, OUT> userFunction;

	private transient TypeSerializer<K> keySerializer;

	private transient AbstractKeyedStateBackend<K> restoredBackend;

	private transient CloseableRegistry registry;

	private transient BufferingCollector<OUT> out;

	private transient Iterator<K> keys;

	/**
	 * Creates an input format for reading partitioned state from an operator in a savepoint.
	 *
	 * @param savepointPath The path to an existing savepoint.
	 * @param uid           The uid of an operator.
	 * @param stateBackend  The state backed used to snapshot the operator.
	 * @param keyType       The type information describing the key type.
	 * @param userFunction  The {@link ProcessReaderFunction} called for each key in the operator.
	 */
	public KeyedStateInputFormat(
		String savepointPath,
		String uid,
		StateBackend stateBackend,
		TypeInformation<K> keyType,
		ProcessReaderFunction<K, OUT> userFunction) {
		super(savepointPath, uid);
		this.stateBackend = stateBackend;
		this.keyType = keyType;
		this.userFunction = userFunction;
	}

	@Override
	public KeyGroupRangeInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		final OperatorState operatorState = getOperatorState();

		return getKeyGroupRangeInputSplits(minNumSplits, operatorState);
	}

	@VisibleForTesting
	static KeyGroupRangeInputSplit[] getKeyGroupRangeInputSplits(int minNumSplits, OperatorState operatorState) {
		final List<KeyGroupRange> keyGroups = partitionKeyGroupRanges(minNumSplits, operatorState);

		final int maxParallelism = operatorState.getMaxParallelism();

		return mapWithIndex(
			keyGroups,
			(keyGroupRange, index) -> createKeyGroupRangeInputSplit(operatorState, maxParallelism, keyGroupRange, index)
		).toArray(KeyGroupRangeInputSplit[]::new);
	}

	private static KeyGroupRangeInputSplit createKeyGroupRangeInputSplit(OperatorState operatorState, int maxParallelism, KeyGroupRange keyGroupRange, Integer index) {
		final List<KeyedStateHandle> handles = StateAssignmentOperation.getManagedKeyedStateHandles(operatorState, keyGroupRange);

		return new KeyGroupRangeInputSplit(handles, keyGroupRange, maxParallelism, index);
	}

	@Override
	protected long getStateSize(OperatorSubtaskState state) {
		return state.getManagedKeyedState().getStateSize();
	}

	@Override
	public void openInputFormat() {
		out = new BufferingCollector<>();
		keySerializer = keyType.createSerializer(getRuntimeContext().getExecutionConfig());
	}

	@Override
	public void open(KeyGroupRangeInputSplit split) throws IOException {
		registry = new CloseableRegistry();

		final Environment environment = new SavepointEnvironment(getRuntimeContext(), new Configuration(), split.getNumKeyGroups());
		restoredBackend = createAndRestoreKeyedStateBackend(
			environment,
			stateBackend,
			keySerializer,
			getUid(),
			split,
			registry);

		final DefaultKeyedStateStore keyedStateStore = new DefaultKeyedStateStore(restoredBackend, getRuntimeContext().getExecutionConfig());
		final SavepointRuntimeContext ctx = new SavepointRuntimeContext(getRuntimeContext(), keyedStateStore);

		ctx.enterOpen();
		List<StateDescriptor<?, ?>> stateDescriptors;
		try {
			FunctionUtils.setFunctionRuntimeContext(userFunction, ctx);
			FunctionUtils.openFunction(userFunction, new Configuration());
			stateDescriptors = ctx.getStateDescriptors();
		} catch (Exception e) {
			throw new IOException("Failed to open user defined function", e);
		}
		ctx.exitOpen();

		this.keys = new MultiStateKeyIterator<>(stateDescriptors, restoredBackend);
	}

	@Override
	public void close() throws IOException {
		registry.unregisterCloseable(restoredBackend);
		restoredBackend.close();
		registry.close();
	}

	@Override
	public boolean reachedEnd() {
		return !out.hasNext() && !keys.hasNext();
	}

	@Override
	public OUT nextRecord(OUT reuse) throws IOException {
		if (out.hasNext()) {
			return out.next();
		}

		final K key = keys.next();
		restoredBackend.setCurrentKey(key);

		try {
			userFunction.processKey(key, out);
		} catch (Exception e) {
			throw new IOException("User defined function ProcessReaderFunction#processKey threw an exception", e);
		}

		keys.remove();

		return out.next();
	}

	/**
	 * Re-partition the key group ranges for n splits using the same logic as if restoring a job with
	 * parallelism n.
	 */
	@Nonnull
	private static List<KeyGroupRange> partitionKeyGroupRanges(int minNumSplits, OperatorState operatorState) {
		return StateAssignmentOperation.createKeyGroupPartitions(
			operatorState.getMaxParallelism(),
			Math.min(minNumSplits, operatorState.getMaxParallelism()));
	}

	@Nonnull
	@VisibleForTesting
	static <K> AbstractKeyedStateBackend<K> createAndRestoreKeyedStateBackend(
		Environment environment,
		StateBackend stateBackend,
		TypeSerializer<K> keySerializer,
		String uid,
		KeyGroupRangeInputSplit split,
		CloseableRegistry closeableRegistry)
		throws IOException {

		CloseableRegistry cancelStreamRegistryForRestore = new CloseableRegistry();
		closeableRegistry.registerCloseable(cancelStreamRegistryForRestore);

		BackendRestorerProcedure<AbstractKeyedStateBackend<K>, KeyedStateHandle> backendRestorer =
			new BackendRestorerProcedure<>(
				(stateHandles) -> stateBackend.createKeyedStateBackend(
					environment,
					environment.getJobID(),
					uid,
					keySerializer,
					split.getNumKeyGroups(),
					split.getKeyGroupRange(),
					environment.getTaskKvStateRegistry(),
					TtlTimeProvider.DEFAULT,
					UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
					stateHandles,
					cancelStreamRegistryForRestore
				),
				closeableRegistry,
				uid
			);

		try {
			return backendRestorer.createAndRestore(split.getHandles());
		} catch (Exception e) {
			throw new IOException("Failed to restore keyed state backend", e);
		} finally {
			if (closeableRegistry.unregisterCloseable(cancelStreamRegistryForRestore)) {
				IOUtils.closeQuietly(cancelStreamRegistryForRestore);
			}
		}
	}
}
