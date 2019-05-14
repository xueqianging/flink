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

package org.apache.flink.connectors.savepoint.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connectors.savepoint.functions.StateBootstapFunction;
import org.apache.flink.connectors.savepoint.operators.StateBootstrapOperator;
import org.apache.flink.connectors.savepoint.runtime.BoundedStreamConfig;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;

import javax.annotation.Nullable;

import java.util.function.Function;

/**
 * {@code OneInputOperator} represents a user defined transformation applied on an {@link Operator}
 * with one input.
 *
 * @param <T> The type of the elements in this operator.
 */
@PublicEvolving
public class OneInputOperator<T> {
	private final DataSet<T> dataSet;

	@Nullable private TimestampAssigner<T> timestampAssigner;

	OneInputOperator(DataSet<T> dataSet) {
		this.dataSet = dataSet;
	}

	/**
	 * Assigns timestamps to the elements in the {@code Operator}.
	 *
	 * @param timestampAssigner The implementation of the timestamp assigner.
	 * @return The stream after the transformation, with assigned timestamps.
	 */
	public OneInputOperator<T> assignTimestamps(TimestampAssigner<T> timestampAssigner) {
		this.timestampAssigner = timestampAssigner;
		return this;
	}

	public OneInputOperator<T> assignTimestamps(Function<T, Long> timestampAssigner) {
		this.timestampAssigner = (element, _unused) -> timestampAssigner.apply(element);
		return this;
	}

	/**
	 * Applies the given {@link StateBootstapFunction} on the non-keyed input.
	 *
	 * <p>The function will be called for every element in the input and can be used for writing
	 * operator state into a {@link Savepoint}.
	 *
	 * @param processFunction The {@link StateBootstapFunction} that is called for each element.
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator process(StateBootstapFunction<T> processFunction) {
		StateBootstrapOperator<T> operator = new StateBootstrapOperator<>(processFunction);

		return transform(operator);
	}

	/**
	 * Method for passing user defined operators along with the type information that will transform
	 * the Operator.
	 *
	 * <p><b>IMPORTANT:</b> Any output from this operator will be discarded.
	 *
	 * @param operator The object containing the transformation logic type of the return stream.
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator transform(OneInputStreamOperator<T, ?> operator) {
		StreamConfig config = new BoundedStreamConfig();
		config.setStreamOperator(operator);

		return new Operator.OneInput<>(dataSet, config, timestampAssigner);
	}

	/**
	 * It creates a new {@link KeyedOperator} that uses the provided key for partitioning its operator
	 * states.
	 *
	 * @param keySelector The KeySelector to be used for extracting the key for partitioning.
	 * @return The {@code Operator} with partitioned state.
	 */
	public <K> KeyedOperator<K, T> keyBy(KeySelector<T, K> keySelector) {
		TypeInformation<K> keyType = TypeExtractor.getKeySelectorTypes(keySelector, dataSet.getType());
		return new KeyedOperator<>(dataSet, keySelector, keyType, timestampAssigner);
	}

	/**
	 * It creates a new {@link KeyedOperator} that uses the provided key with explicit type
	 * information for partitioning its operator states.
	 *
	 * @param keySelector The KeySelector to be used for extracting the key for partitioning.
	 * @param keyType The type information describing the key type.
	 * @return The {@code Operator} with partitioned state.
	 */
	public <K> KeyedOperator<K, T> keyBy(KeySelector<T, K> keySelector, TypeInformation<K> keyType) {
		return new KeyedOperator<>(dataSet, keySelector, keyType, timestampAssigner);
	}

	/**
	 * Partitions the operator state of a {@link Operator} by the given key positions.
	 *
	 * @param fields The position of the fields on which the {@code Operator} will be grouped.
	 * @return The {@code Operator} with partitioned state.
	 */
	public KeyedOperator<Tuple, T> keyBy(int... fields) {
		if (dataSet.getType() instanceof BasicArrayTypeInfo || dataSet.getType() instanceof PrimitiveArrayTypeInfo) {
			return keyBy(KeySelectorUtil.getSelectorForArray(fields, dataSet.getType()));
		} else {
			return keyBy(new Keys.ExpressionKeys<>(fields, dataSet.getType()));
		}
	}

	/**
	 * Partitions the operator state of a {@link Operator} using field expressions. A field expression
	 * is either the name of a public field or a getter method with parentheses of the {@code
	 * Operator}'s underlying type. A dot can be used to drill down into objects, as in {@code
	 * "field1.getInnerField2()" }.
	 *
	 * @param fields One or more field expressions on which the state of the {@link Operator}
	 *     operators will be partitioned.
	 * @return The {@code Operator} with partitioned state (i.e. KeyedStream)
	 */
	public KeyedOperator<Tuple, T> keyBy(String... fields) {
		return keyBy(new Keys.ExpressionKeys<>(fields, dataSet.getType()));
	}

	private KeyedOperator<Tuple, T> keyBy(Keys<T> keys) {
		KeySelector<T, Tuple> keySelector = KeySelectorUtil.getSelectorForKeys(
			keys,
			dataSet.getType(),
			dataSet.getExecutionEnvironment().getConfig());

		TypeInformation<Tuple> keyType = TypeExtractor.getKeySelectorTypes(keySelector, dataSet.getType());
		return new KeyedOperator<>(dataSet, keySelector, keyType, timestampAssigner);
	}
}

