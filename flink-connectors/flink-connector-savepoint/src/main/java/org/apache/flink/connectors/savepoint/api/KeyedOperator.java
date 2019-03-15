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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connectors.savepoint.functions.KeyedProcessWriterFunction;
import org.apache.flink.connectors.savepoint.operators.KeyedProcessWriterOperator;
import org.apache.flink.connectors.savepoint.runtime.BoundedStreamConfig;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import javax.annotation.Nullable;

/**
 * A {@link KeyedOperator} represents a {@link OneInputOperator} on which operator state is
 * partitioned by key using a provided {@link KeySelector}.
 *
 * @param <K> The type of the key in the Keyed Operator.
 * @param <T> The type of the elements in the Keyed Operator.
 */
@PublicEvolving
public class KeyedOperator<K, T> {
	private final DataSet<T> dataSet;

	private final KeySelector<T, K> keySelector;

	private final TypeInformation<K> keyType;

	@Nullable private final TimestampAssigner<T> timestampAssigner;

	KeyedOperator(
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
	 * Windows this {@code KeyedOperator} into into sliding time windows.
	 *
	 * <p>If a {@link TimestampAssigner} has been set for the operator than {@link
	 * SlidingEventTimeWindows} is used, otherwise {@link SlidingProcessingTimeWindows}.
	 *
	 * @param size The size of the window.
	 */
	public WindowedOperator<K, TimeWindow, T> timeWindow(Time size) {
		WindowAssigner<Object, TimeWindow> windowAssigner;

		if (timestampAssigner != null) {
			windowAssigner = TumblingEventTimeWindows.of(size);
		} else {
			windowAssigner = TumblingProcessingTimeWindows.of(size);
		}

		return new WindowedOperator<>(dataSet, keySelector, keyType, windowAssigner, timestampAssigner);
	}

	/**
	 * Windows this {@code KeyedOperator} into tumbling time windows.
	 *
	 * <p>If a {@link TimestampAssigner} has been set for the operator than {@link
	 * TumblingEventTimeWindows} is used, otherwise {@link TumblingProcessingTimeWindows}.
	 *
	 * @param size The size of the window.
	 */
	public WindowedOperator<K, TimeWindow, T> timeWindow(Time size, Time slide) {
		WindowAssigner<Object, TimeWindow> windowAssigner;

		if (timestampAssigner != null) {
			windowAssigner = SlidingEventTimeWindows.of(size, slide);
		} else {
			windowAssigner = SlidingProcessingTimeWindows.of(size, slide);
		}

		return new WindowedOperator<>(dataSet, keySelector, keyType, windowAssigner, timestampAssigner);
	}

	/**
	 * Applies the given {@link KeyedProcessWriterFunction} on the keyed input.
	 *
	 * <p>The function will be called for every element in the input and can be used for writing both
	 * keyed and operator state into a {@link Savepoint}.
	 *
	 * @param processFunction The {@link KeyedProcessWriterFunction} that is called for each element.
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator process(KeyedProcessWriterFunction<K, T> processFunction) {
		KeyedProcessWriterOperator<K, T> operator = new KeyedProcessWriterOperator<>(processFunction);

		return transform(operator);
	}

	/**
	 * Method for passing user defined operators along with the type information that will transform
	 * the Operator.
	 *
	 * <p><b>IMPORTANT:</b> Any output from this operator will be discarded.
	 *
	 * @param operator The object containing the transformation logic type of the return stream
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator transform(OneInputStreamOperator<T, ?> operator) {
		TypeSerializer<K> keySerializer =
			keyType.createSerializer(dataSet.getExecutionEnvironment().getConfig());

		StreamConfig config = new BoundedStreamConfig(keySerializer, keySelector);
		config.setStreamOperator(operator);

		return new Operator.OneInput<>(dataSet, config, keySelector, timestampAssigner);
	}
}

