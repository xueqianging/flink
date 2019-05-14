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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.savepoint.functions.ProcessReaderFunction;
import org.apache.flink.connectors.savepoint.output.OperatorStateReducer;
import org.apache.flink.connectors.savepoint.output.OperatorSubtaskStateReducer;
import org.apache.flink.connectors.savepoint.output.SavepointOutputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.StateBackend;

import java.util.HashMap;
import java.util.Map;

/**
 * A new savepoint.
 */
public class NewSavepoint implements Savepoint {
	private final StateBackend stateBackend;

	private final int maxParallelism;

	private final Map<String, OperatorTransformation> operatorsByUid;

	NewSavepoint(StateBackend stateBackend, int maxParallelism) {
		this.stateBackend = stateBackend;
		this.maxParallelism = maxParallelism;
		this.operatorsByUid = new HashMap<>();
	}

	@Override
	public <T> DataSet<T> readListState(String uid, String name, TypeInformation<T> typeInfo) {
		throw new IllegalStateException("State can only be read from an existing savepoint");
	}

	@Override
	public <T> DataSet<T> readListState(String uid, String name, TypeInformation<T> typeInfo, TypeSerializer<T> serializer) {
		throw new IllegalStateException("State can only be read from an existing savepoint");
	}

	@Override
	public <T> DataSet<T> readUnionState(String uid, String name, TypeInformation<T> typeInfo) {
		throw new IllegalStateException("State can only be read from an existing savepoint");
	}

	@Override
	public <T> DataSet<T> readUnionState(String uid, String name, TypeInformation<T> typeInfo, TypeSerializer<T> serializer) {
		throw new IllegalStateException("State can only be read from an existing savepoint");
	}

	@Override
	public <K, V> DataSet<Tuple2<K, V>> readBroadcastState(String uid, String name, TypeInformation<K> keyTypeInfo, TypeInformation<V> valueTypeInfo) {
		throw new IllegalStateException("State can only be read from an existing savepoint");
	}

	@Override
	public <K, V> DataSet<Tuple2<K, V>> readBroadcastState(String uid, String name, TypeInformation<K> keyTypeInfo, TypeInformation<V> valueTypeInfo, TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer) {
		throw new IllegalStateException("State can only be read from an existing savepoint");
	}

	@Override
	public <K, OUT> DataSet<OUT> readKeyedState(String uid, ProcessReaderFunction<K, OUT> function) {
		throw new IllegalStateException("State can only be read from an existing savepoint");
	}

	@Override
	public <K, OUT> DataSet<OUT> readKeyedState(String uid, ProcessReaderFunction<K, OUT> function, TypeInformation<K> keyTypeInfo, TypeInformation<OUT> outTypeInfo) {
		throw new IllegalStateException("State can only be read from an existing savepoint");
	}

	@Override
	public Savepoint removeOperator(String uid) {
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

		operatorsByUid
			.entrySet()
			.stream()
			.map(entry -> getOperatorStates(entry.getKey(), entry.getValue(), savepointPath))
			.reduce(DataSet::union)
			.orElseThrow(() -> new IllegalStateException("Savepoint's must contain at least one operator"))
			.reduceGroup(new OperatorStateReducer())
			.output(new SavepointOutputFormat(savepointPath));
	}

	private DataSet<OperatorState> getOperatorStates(String uid, OperatorTransformation operator, Path savepointPath) {
		MaxParallelismSupplier supplier = MaxParallelismSupplier.of(maxParallelism);

		return operator
			.getOperatorSubtaskStates(uid, stateBackend, supplier, savepointPath)
			.reduceGroup(new OperatorSubtaskStateReducer(uid, supplier));
	}
}
