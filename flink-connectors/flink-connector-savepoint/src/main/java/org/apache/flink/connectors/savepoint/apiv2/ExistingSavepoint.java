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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.InvalidTypesException;
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
import org.apache.flink.connectors.savepoint.functions.KeyedStateReaderFunction;
import org.apache.flink.connectors.savepoint.input.BroadcastStateInputFormat;
import org.apache.flink.connectors.savepoint.input.KeyedStateInputFormat;
import org.apache.flink.connectors.savepoint.input.ListStateInputFormat;
import org.apache.flink.connectors.savepoint.input.OperatorInputFormat;
import org.apache.flink.connectors.savepoint.input.UnionStateInputFormat;
import org.apache.flink.connectors.savepoint.output.MaxParallelismSupplier;
import org.apache.flink.connectors.savepoint.output.OnDiskMaxParallelismSupplier;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateBackend;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An existing savepoint.
 */
@SuppressWarnings("WeakerAccess")
@PublicEvolving
public class ExistingSavepoint extends WritableSavepoint<ExistingSavepoint> {
	private final ExecutionEnvironment env;

	private final String existingSavepoint;

	private final StateBackend stateBackend;

	private final Map<String, BootstrapTransformation> transformations;

	private final List<String> droppedOperators;

	ExistingSavepoint(ExecutionEnvironment env, String path, StateBackend stateBackend) {
		this.env = env;
		this.existingSavepoint = path;
		this.stateBackend = stateBackend;
		this.transformations = new HashMap<>();
		this.droppedOperators = new ArrayList<>();
	}

	/**
	 * @return The {@link OperatorID}'s in the savepoint.
	 */
	public DataSet<OperatorID> listOperatorIDs() {
		return env
			.createInput(new OperatorInputFormat(existingSavepoint, Collections.emptyList()))
			.map(OperatorState::getOperatorID);
	}

	/**
	 * Read operator {@code ListState} from a {@code Savepoint}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the elements in the state.
	 * @param <T> The type of the values that are in the list state.
	 * @return A {@code DataSet} representing the elements in state.
	 */
	public <T> DataSet<T> readListState(String uid, String name, TypeInformation<T> typeInfo) {
		ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, typeInfo);
		ListStateInputFormat<T> inputFormat = new ListStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, typeInfo);
	}

	/**
	 * Read operator {@code ListState} from a {@code Savepoint} when a
	 * custom serializer was used; e.g., a different serializer than the
	 * one returned by {@code TypeInformation#createSerializer}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the elements in the state.
	 * @param serializer The serializer used to write the elements into state.
	 * @param <T> The type of the values that are in the list state.
	 * @return A {@code DataSet} representing the elements in state.
	 */
	public <T> DataSet<T> readListState(
		String uid,
		String name,
		TypeInformation<T> typeInfo,
		TypeSerializer<T> serializer) {

		ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, serializer);
		ListStateInputFormat<T> inputFormat = new ListStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, typeInfo);
	}

	/**
	 * Read operator {@code UnionState} from a {@code Savepoint}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the elements in the state.
	 * @param <T> The type of the values that are in the union state.
	 * @return A {@code DataSet} representing the elements in state.
	 */
	public <T> DataSet<T> readUnionState(String uid, String name, TypeInformation<T> typeInfo) {
		ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, typeInfo);
		UnionStateInputFormat<T> inputFormat = new UnionStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, typeInfo);
	}

	/**
	 * Read operator {@code UnionState} from a {@code Savepoint} when a
	 * custom serializer was used; e.g., a different serializer than the
	 * one returned by {@code TypeInformation#createSerializer}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the elements in the state.
	 * @param serializer The serializer used to write the elements into state.
	 * @param <T> The type of the values that are in the union state.
	 * @return A {@code DataSet} representing the elements in state.
	 */
	public <T> DataSet<T> readUnionState(
		String uid,
		String name,
		TypeInformation<T> typeInfo,
		TypeSerializer<T> serializer) {

		ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, serializer);
		UnionStateInputFormat<T> inputFormat = new UnionStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, typeInfo);
	}

	/**
	 * Read operator {@code BroadcastState} from a {@code Savepoint}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param keyTypeInfo The type information for the keys in the state.
	 * @param valueTypeInfo The type information for the values in the state.
	 * @param <K> The type of keys in state.
	 * @param <V> The type of values in state.
	 * @return A {@code DataSet} of key-value pairs from state.
	 */
	public <K, V> DataSet<Tuple2<K, V>> readBroadcastState(
		String uid,
		String name,
		TypeInformation<K> keyTypeInfo,
		TypeInformation<V> valueTypeInfo) {

		MapStateDescriptor<K, V> descriptor = new MapStateDescriptor<>(name, keyTypeInfo, valueTypeInfo);
		BroadcastStateInputFormat<K, V> inputFormat = new BroadcastStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, new TupleTypeInfo<>(keyTypeInfo, valueTypeInfo));
	}

	/**
	 * Read operator {@code BroadcastState} from a {@code Savepoint}
	 * when a custom serializer was used; e.g., a different serializer
	 * than the one returned by {@code TypeInformation#createSerializer}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param keyTypeInfo The type information for the keys in the state.
	 * @param valueTypeInfo The type information for the values in the state.
	 * @param keySerializer The type serializer used to write keys into the state.
	 * @param valueSerializer The type serializer used to write values into the state.
	 * @param <K> The type of keys in state.
	 * @param <V> The type of values in state.
	 * @return A {@code DataSet} of key-value pairs from state.
	 */
	public <K, V> DataSet<Tuple2<K, V>> readBroadcastState(
		String uid,
		String name,
		TypeInformation<K> keyTypeInfo,
		TypeInformation<V> valueTypeInfo,
		TypeSerializer<K> keySerializer,
		TypeSerializer<V> valueSerializer) {

		MapStateDescriptor<K, V> descriptor = new MapStateDescriptor<>(name, keySerializer, valueSerializer);
		BroadcastStateInputFormat<K, V> inputFormat = new BroadcastStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, new TupleTypeInfo<>(keyTypeInfo, valueTypeInfo));
	}

	/**
	 * Read keyed state from an operator in a {@code Savepoint}.
	 * @param uid The uid of the operator.
	 * @param function The {@link KeyedStateReaderFunction} that is called for each key in state.
	 * @param <K> The type of the key in state.
	 * @param <OUT> The output type of the transform function.
	 * @return A {@code DataSet} of objects read from keyed state.
	 */
	public <K, OUT> DataSet<OUT> readKeyedState(String uid, KeyedStateReaderFunction<K, OUT> function) {

		TypeInformation<K> keyTypeInfo;
		TypeInformation<OUT> outType;

		try {
			keyTypeInfo = TypeExtractor.createTypeInfo(
				KeyedStateReaderFunction.class,
				function.getClass(), 0, null, null);
		} catch (InvalidTypesException e) {
			throw new InvalidProgramException(
				"The key type of the KeyedStateReaderFunction could not be automatically determined. Please use " +
					"Savepoint#readKeyedState(String, KeyedStateReaderFunction, TypeInformation, TypeInformation) instead.", e);
		}

		try {
			outType = TypeExtractor.getUnaryOperatorReturnType(
				function,
				KeyedStateReaderFunction.class,
				0,
				1,
				TypeExtractor.NO_INDEX,
				keyTypeInfo,
				Utils.getCallLocationName(),
				false);
		} catch (InvalidTypesException e) {
			throw new InvalidProgramException(
				"The output type of the KeyedStateReaderFunction could not be automatically determined. Please use " +
				"Savepoint#readKeyedState(String, KeyedStateReaderFunction, TypeInformation, TypeInformation) instead.", e);
		}

		return readKeyedState(uid, function, keyTypeInfo, outType);
	}

	/**
	 * Read keyed state from an operator in a {@code Savepoint}.
	 * @param uid The uid of the operator.
	 * @param function The {@link KeyedStateReaderFunction} that is called for each key in state.
	 * @param keyTypeInfo The type information of the key in state.
	 * @param outTypeInfo The type information of the output of the transform reader function.
	 * @param <K> The type of the key in state.
	 * @param <OUT> The output type of the transform function.
	 * @return A {@code DataSet} of objects read from keyed state.
	 */
	public <K, OUT> DataSet<OUT> readKeyedState(
		String uid,
		KeyedStateReaderFunction<K, OUT> function,
		TypeInformation<K> keyTypeInfo,
		TypeInformation<OUT> outTypeInfo) {

		KeyedStateInputFormat<K, OUT> inputFormat = new KeyedStateInputFormat<>(
			existingSavepoint,
			uid,
			stateBackend,
			keyTypeInfo,
			function);

		return env.createInput(inputFormat, outTypeInfo);
	}

	@Override
	public ExistingSavepoint removeOperator(String uid) {
		droppedOperators.add(uid);
		transformations.remove(uid);
		return this;
	}

	@Override
	public <T> ExistingSavepoint withOperator(String uid, BootstrapTransformation<T> transformation) {
		if (transformations.containsKey(uid)) {
			throw new IllegalArgumentException("Duplicate uid " + uid + ". All uid's must be unique");
		}

		transformations.put(uid, transformation);
		return this;
	}

	@Override
	public void write(String path) {
		Path savepointPath = new Path(path);

		MaxParallelismSupplier supplier = new OnDiskMaxParallelismSupplier(path);

		DataSet<OperatorState> existingOperators = env.createInput(
			new OperatorInputFormat(existingSavepoint, droppedOperators));

		write(savepointPath, transformations, stateBackend, supplier, existingOperators);
	}
}
