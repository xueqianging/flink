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

package org.apache.flink.connectors.savepoint.apiv2;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.savepoint.functions.ProcessReaderFunction;
import org.apache.flink.runtime.state.StateBackend;

/**
 * A {@link Savepoint} is a collection of operator states that can be used to supply initial state
 * when starting a {@link org.apache.flink.streaming.api.datastream.DataStream} job.
 */
public interface Savepoint {

	/**
	 * Loads an existing savepoint. Useful if you want to query, modify, or extend
	 * the state of an existing application.
	 *
	 * <p><b>IMPORTANT</b> State is shared between savepoints by copying pointers, no deep copy is
	 * performed. If two savepoint's share operators then one cannot be discarded without corrupting
	 * the other.
	 *
	 * @param env The execution enviornment used to process the savepoint.
	 * @param path The path to an existing savepoint on disk.
	 * @param stateBackend The state backend of the savepoint used for keyed state.
	 */
	static Savepoint load(ExecutionEnvironment env, String path, StateBackend stateBackend) {
		return new ExistingSavepoint(env, path, stateBackend);
	}

	/**
	 * Creates a new savepoint.
	 *
	 * @param stateBackend The state backend of the savepoint used for keyed state.
	 * @param maxParallelism The max parallelism of the savepoint.
	 * @return A new savepoint.
	 */
	static Savepoint create(StateBackend stateBackend, int maxParallelism) {
		return new NewSavepoint(stateBackend, maxParallelism);
	}

	/**
	 * Read operator {@code ListState} from a {@code Savepoint}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the elements in the state.
	 * @param <T> The type of the values that are in the list state.
	 * @return A {@code DataSet} representing the elements in state.
	 */
	<T> DataSet<T> readListState(
		String uid,
		String name,
		TypeInformation<T> typeInfo);

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
	<T> DataSet<T> readListState(
		String uid,
		String name,
		TypeInformation<T> typeInfo,
		TypeSerializer<T> serializer);

	/**
	 * Read operator {@code UnionState} from a {@code Savepoint}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the elements in the state.
	 * @param <T> The type of the values that are in the union state.
	 * @return A {@code DataSet} representing the elements in state.
	 */
	<T> DataSet<T> readUnionState(
		String uid,
		String name,
		TypeInformation<T> typeInfo);

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
	<T> DataSet<T> readUnionState(
		String uid,
		String name,
		TypeInformation<T> typeInfo,
		TypeSerializer<T> serializer);

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
	<K, V> DataSet<Tuple2<K, V>> readBroadcastState(
		String uid,
		String name,
		TypeInformation<K> keyTypeInfo,
		TypeInformation<V> valueTypeInfo);

	/**
	 * Read operator {@code BroadcastState} from a {@code Savepoint}.
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
	<K, V> DataSet<Tuple2<K, V>> readBroadcastState(
		String uid,
		String name,
		TypeInformation<K> keyTypeInfo,
		TypeInformation<V> valueTypeInfo,
		TypeSerializer<K> keySerializer,
		TypeSerializer<V> valueSerializer);

	/**
	 * Read keyed state from an operator in a {@code Savepoint}.
	 * @param uid The uid of the operator.
	 * @param function The {@link ProcessReaderFunction} that is called for each key in state.
	 * @param <K> The type of the key in state.
	 * @param <OUT> The output type of the process function.
	 * @return A {@code DataSet} of objects read from keyed state.
	 */
	<K, OUT> DataSet<OUT> readKeyedState(
		String uid,
		ProcessReaderFunction<K, OUT> function);

	/**
	 * Read keyed state from an operator in a {@code Savepoint}.
	 * @param uid The uid of the operator.
	 * @param function The {@link ProcessReaderFunction} that is called for each key in state.
	 * @param keyTypeInfo The type information of the key in state.
	 * @param outTypeInfo The type information of the output of the process reader function.
	 * @param <K> The type of the key in state.
	 * @param <OUT> The output type of the process function.
	 * @return A {@code DataSet} of objects read from keyed state.
	 */
	<K, OUT> DataSet<OUT> readKeyedState(
		String uid,
		ProcessReaderFunction<K, OUT> function,
		TypeInformation<K> keyTypeInfo,
		TypeInformation<OUT> outTypeInfo);

	/**
	 * Drop an existing operator from the savepoint.
	 * @param uid The uid of the operator.
	 * @return A modified savepoint.
	 */
	Savepoint removeOperator(String uid);

	/**
	 * Adds a new operator to the savepoint.
	 * @param uid The uid of the operator.
	 * @param operatorTransformation The operator to be included.
	 * @return The modified savepoint.
	 */
	Savepoint withOperator(String uid, OperatorTransformation operatorTransformation);

	/**
	 * Write out a new or updated savepoint.
	 * @param path The path to where the savepoint should be written.
	 */
	void write(String path);
}
