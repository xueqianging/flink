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

package org.apache.flink.connectors.savepoint.apiv2;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connectors.savepoint.functions.ProcessReaderFunction;
import org.apache.flink.connectors.savepoint.input.BroadcastStateInputFormat;
import org.apache.flink.connectors.savepoint.input.KeyedStateInputFormat;
import org.apache.flink.connectors.savepoint.input.ListStateInputFormat;
import org.apache.flink.connectors.savepoint.input.UnionStateInputFormat;
import org.apache.flink.connectors.savepoint.output.OperatorStateReducer;
import org.apache.flink.connectors.savepoint.output.OperatorSubtaskStateReducer;
import org.apache.flink.connectors.savepoint.output.SavepointOutputFormat;
import org.apache.flink.connectors.savepoint.runtime.OperatorIDGenerator;
import org.apache.flink.connectors.savepoint.runtime.SavepointLoader;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An existing savepoint.
 */
public class ExistingSavepoint implements Savepoint {
	private final ExecutionEnvironment env;

	private final String existingSavepoint;

	private final StateBackend stateBackend;

	private final Map<String, OperatorTransformation> operatorsByUid;

	private final List<String> droppedOperators;

	ExistingSavepoint(ExecutionEnvironment env, String path, StateBackend stateBackend) {
		this.env = env;
		this.existingSavepoint = path;
		this.stateBackend = stateBackend;
		this.operatorsByUid = new HashMap<>();
		this.droppedOperators = new ArrayList<>();
	}

	@Override
	public <T> DataSet<T> readListState(String uid, String name, TypeInformation<T> typeInfo) {
		ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, typeInfo);
		ListStateInputFormat<T> inputFormat = new ListStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, typeInfo);
	}

	@Override
	public <T> DataSet<T> readListState(String uid, String name, TypeInformation<T> typeInfo, TypeSerializer<T> serializer) {
		ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, serializer);
		ListStateInputFormat<T> inputFormat = new ListStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, typeInfo);
	}

	@Override
	public <T> DataSet<T> readUnionState(String uid, String name, TypeInformation<T> typeInfo) {
		ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, typeInfo);
		UnionStateInputFormat<T> inputFormat = new UnionStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, typeInfo);
	}

	@Override
	public <T> DataSet<T> readUnionState(String uid, String name, TypeInformation<T> typeInfo, TypeSerializer<T> serializer) {
		ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, serializer);
		UnionStateInputFormat<T> inputFormat = new UnionStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, typeInfo);
	}

	@Override
	public <K, V> DataSet<Tuple2<K, V>> readBroadcastState(String uid, String name, TypeInformation<K> keyTypeInfo, TypeInformation<V> valueTypeInfo) {
		MapStateDescriptor<K, V> descriptor = new MapStateDescriptor<>(name, keyTypeInfo, valueTypeInfo);
		BroadcastStateInputFormat<K, V> inputFormat = new BroadcastStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, new TupleTypeInfo<>(keyTypeInfo, valueTypeInfo));
	}

	@Override
	public <K, V> DataSet<Tuple2<K, V>> readBroadcastState(String uid, String name, TypeInformation<K> keyTypeInfo, TypeInformation<V> valueTypeInfo, TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer) {
		MapStateDescriptor<K, V> descriptor = new MapStateDescriptor<>(name, keySerializer, valueSerializer);
		BroadcastStateInputFormat<K, V> inputFormat = new BroadcastStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, new TupleTypeInfo<>(keyTypeInfo, valueTypeInfo));
	}

	@Override
	public <K, OUT> DataSet<OUT> readKeyedState(String uid, ProcessReaderFunction<K, OUT> function) {
		TypeInformation<K> keyTypeInfo;
		TypeInformation<OUT> outType;

		try {
			keyTypeInfo = TypeExtractor.createTypeInfo(
				ProcessReaderFunction.class,
				function.getClass(), 0, null, null);
		} catch (InvalidTypesException e) {
			throw new InvalidProgramException(
				"The key type of the ProcessReaderFunction could not be automatically determined. Please use " +
					"Savepoint#readKeyedState(String, ProcessReaderFunction, TypeInformation, TypeInformation) instead.", e);
		}

		try {
			outType = TypeExtractor.getUnaryOperatorReturnType(
				function,
				ProcessReaderFunction.class,
				0,
				1,
				TypeExtractor.NO_INDEX,
				keyTypeInfo,
				Utils.getCallLocationName(),
				false);
		} catch (InvalidTypesException e) {
			throw new InvalidProgramException(
				"The output type of the ProcessReaderFunction could not be automatically determined. Please use " +
				"Savepoint#readKeyedState(String, ProcessReaderFunction, TypeInformation, TypeInformation) instead.", e);
		}

		return readKeyedState(uid, function, keyTypeInfo, outType);
	}

	@Override
	public <K, OUT> DataSet<OUT> readKeyedState(String uid, ProcessReaderFunction<K, OUT> function, TypeInformation<K> keyTypeInfo, TypeInformation<OUT> outTypeInfo) {
		KeyedStateInputFormat<K, OUT> inputFormat = new KeyedStateInputFormat<>(existingSavepoint, uid, stateBackend, keyTypeInfo, function);
		return env.createInput(inputFormat, outTypeInfo);
	}

	@Override
	public Savepoint removeOperator(String uid) {
		droppedOperators.add(uid);
		operatorsByUid.remove(uid);
		return this;
	}

	@Override
	public Savepoint withOperator(String uid, OperatorTransformation operatorTransformation) {
		if (operatorsByUid.containsKey(uid)) {
			throw new IllegalArgumentException("Duplicate uid " + uid + ". All uid's must be unique");
		}

		operatorsByUid.put(uid, operatorTransformation);
		return this;
	}

	@Override
	public void write(String path) {
		Path savepointPath = new Path(path);

		DataSet<OperatorState> existingOperators = env
			.createInput(new OperatorStateInput(existingSavepoint, droppedOperators), TypeInformation.of(OperatorState.class));

		operatorsByUid
			.entrySet()
			.stream()
			.map(entry -> getOperatorStates(entry.getKey(), entry.getValue(), savepointPath))
			.reduce(DataSet::union)
			.orElseThrow(() -> new IllegalStateException("Savepoint's must contain at least one operator"))
			.union(existingOperators)
			.reduceGroup(new OperatorStateReducer())
			.output(new SavepointOutputFormat(savepointPath));
	}

	private DataSet<OperatorState> getOperatorStates(String uid, OperatorTransformation operator, Path savepointPath) {
		OnDiskMaxParallelismSupplier supplier = new OnDiskMaxParallelismSupplier(existingSavepoint);

		return operator
			.getOperatorSubtaskStates(uid, stateBackend, supplier, savepointPath)
			.reduceGroup(new OperatorSubtaskStateReducer(uid, supplier));
	}

	private static class OperatorStateInput extends GenericInputFormat<OperatorState> implements NonParallelInput {

		private final String path;

		private final Set<OperatorID> droppedOperators;

		private transient Iterator<OperatorState> inner;

		private OperatorStateInput(String path, List<String> droppedOperators) {
			this.path = path;
			this.droppedOperators = droppedOperators
				.stream()
				.map(OperatorIDGenerator::fromUid)
				.collect(Collectors.toSet());
		}

		@Override
		public void open(GenericInputSplit split) throws IOException {
			try {
				inner = SavepointLoader
					.loadSavepoint(path, getRuntimeContext().getUserCodeClassLoader())
					.getOperatorStates()
					.stream()
					.filter(state -> !droppedOperators.contains(state.getOperatorID()))
					.iterator();
			} catch (IOException e) {
				throw new IOException("Failed to load operators from existing savepoint", e);
			}
		}

		@Override
		public boolean reachedEnd() throws IOException {
			return !inner.hasNext();
		}

		@Override
		public OperatorState nextRecord(OperatorState reuse) throws IOException {
			return inner.next();
		}
	}

	/**
	 * An supplier that returns the max parallelism
	 * of an existing savepoint.
	 *
	 * <b>IMPORTANT:</b> The savepoint must not be loaded on the cluster to
	 * ensure all filesystem's are on the classpath.
	 */
	private static class OnDiskMaxParallelismSupplier implements MaxParallelismSupplier {

		private final String path;

		private OnDiskMaxParallelismSupplier(String path) {
			this.path = path;
		}

		@Override
		public Integer get() {
			try {
				return SavepointLoader
					.loadSavepoint(path, this.getClass().getClassLoader())
					.getOperatorStates()
					.stream()
					.map(OperatorState::getMaxParallelism)
					.max(Comparator.naturalOrder())
					.orElseThrow(() -> new RuntimeException("Savepoint's must contain at least one operator"));
			} catch (IOException e) {
				throw new FlinkRuntimeException("Failed to calculate max parallelism from existing savepoint", e);
			}
		}
	}
}
