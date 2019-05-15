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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connectors.savepoint.functions.KeyedStateBootstrapFunction;
import org.apache.flink.connectors.savepoint.operators.KeyedStateBootstrapOperator;
import org.apache.flink.connectors.savepoint.runtime.BoundedStreamConfig;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;

import javax.annotation.Nullable;

/**
 * A {@link KeyedOperatorTransformation} represents a {@link OneInputOperatorTransformation} on which operator state is
 * partitioned by key using a provided {@link KeySelector}.
 *
 * @param <K> The type of the key in the Keyed OperatorTransformation.
 * @param <T> The type of the elements in the Keyed OperatorTransformation.
 */
@PublicEvolving
@SuppressWarnings("WeakerAccess")
public class KeyedOperatorTransformation<K, T> {
	private final DataSet<T> dataSet;

	private final KeySelector<T, K> keySelector;

	private final TypeInformation<K> keyType;

	@Nullable private final TimestampAssigner<T> timestampAssigner;

	KeyedOperatorTransformation(
		DataSet<T> dataSet,
		KeySelector<T, K> keySelector,
		TypeInformation<K> keyType,
		@Nullable TimestampAssigner<T> timestampAssigner) {
		this.dataSet = dataSet;
		this.keySelector = keySelector;
		this.keyType = keyType;
		this.timestampAssigner = timestampAssigner;
	}

	/**
	 * Applies the given {@link KeyedStateBootstrapFunction} on the keyed input.
	 *
	 * <p>The function will be called for every element in the input and can be used for writing both
	 * keyed and operator state into a {@link Savepoint}.
	 *
	 * @param processFunction The {@link KeyedStateBootstrapFunction} that is called for each element.
	 * @return An {@link OperatorTransformation} that can be added to a {@link Savepoint}.
	 */
	public BootstrapTransformation<T> transform(KeyedStateBootstrapFunction<K, T> processFunction) {
		KeyedStateBootstrapOperator<K, T> operator = new KeyedStateBootstrapOperator<>(processFunction);

		return transform(operator);
	}

	/**
	 * Method for passing user defined operators along with the type information that will transform
	 * the OperatorTransformation.
	 *
	 * <p><b>IMPORTANT:</b> Any output from this operator will be discarded.
	 *
	 * @param operator The object containing the transformation logic type of the return stream
	 * @return An {@link OperatorTransformation} that can be added to a {@link Savepoint}.
	 */
	public BootstrapTransformation<T> transform(OneInputStreamOperator<T, ?> operator) {
		TypeSerializer<K> keySerializer =
			keyType.createSerializer(dataSet.getExecutionEnvironment().getConfig());

		StreamConfig config = new BoundedStreamConfig(keySerializer, keySelector);
		config.setStreamOperator(operator);

		return new BootstrapTransformation<>(dataSet, config, keySelector, timestampAssigner);
	}
}

