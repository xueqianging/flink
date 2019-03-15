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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connectors.savepoint.runtime.BoundedStreamConfig;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.functions.aggregation.SumAggregator;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyWindowFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.EvictingWindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableProcessWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;


/**
 * A {@code WindowedOperator} represents an operator where elements are grouped by key, and for each
 * key, the stream of elements is split into windows based on a {@link
 * org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}. Window emission is triggered
 * based on a {@link org.apache.flink.streaming.api.windowing.triggers.Trigger}.
 *
 * <p>The windows are conceptually evaluated for each key individually, meaning windows can trigger
 * at different points for each key.
 *
 * <p>Note that the {@code WindowedOperator} is purely and API construct, during runtime the {@code
 * WindowedStream} will be collapsed together with the {@code KeyedOperator} and the operation over
 * the window into one single operation.
 *
 * <p><b>NOTE:</b>{@code WindowedOperator} currently only supports {@link
 * org.apache.flink.streaming.api.windowing.windows.TimeWindow}'s.
 *
 * @param <T> The type of elements in the stream.
 * @param <K> The type of the key by which elements are grouped.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns the elements to.
 */
@PublicEvolving
@SuppressWarnings("WeakerAccess")
public class WindowedOperator<K, W extends Window, T> {
	// TODO the string "window-contents" should be a 'public static final' variable inside of
	// WindowOperator
	private static final String STATE_DESCRIPTOR_NAME = "window-contents";

	/**
	 * Bounded streams do not carry watermarks so data is never late.
	 *
	 * <p>Allowed lateness is not stored in checkpoints so it may be set on the {@link
	 * org.apache.flink.streaming.api.datastream.WindowedStream} that is restored from this state.
	 */
	private static final long ALLOWED_LATENESS = 0L;

	/**
	 * Bounded streams do not carry watermarks so data is never late.
	 *
	 * <p>{@link OutputTag}'s are not stored in checkpoints so it may be set on the {@link
	 * org.apache.flink.streaming.api.datastream.WindowedStream} that is restored from this state.
	 */
	private static <T> OutputTag<T> late_data_output_tag() {
		return null;
	}

	private final DataSet<T> dataSet;

	private final KeySelector<T, K> keySelector;

	private final TypeInformation<K> keyType;

	private final WindowAssigner<? super T, W> windowAssigner;

	@Nullable private final TimestampAssigner<T> timestampAssigner;

	WindowedOperator(
		DataSet<T> dataSet,
		KeySelector<T, K> keySelector,
		TypeInformation<K> keyType,
		WindowAssigner<? super T, W> windowAssigner,
		@Nullable TimestampAssigner<T> timestampAssigner) {
		this.dataSet = dataSet;
		this.keySelector = keySelector;
		this.keyType = keyType;
		this.windowAssigner = windowAssigner;
		this.timestampAssigner = timestampAssigner;
	}

	/**
	 * Applies a reduce function to the window. The window function is called for each evaluation of
	 * the window for each key individually.
	 *
	 * <p>This window will try and incrementally aggregate data as much as the window policies permit.
	 * For example, tumbling time windows can aggregate the data, meaning that only one element per
	 * key is stored. Sliding time windows will aggregate on the granularity of the slide interval, so
	 * a few elements are stored per key (one per slide interval). Custom windows may not be able to
	 * incrementally aggregate, or may need to store extra values in an aggregation tree.
	 *
	 * @param function The reduce function.
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator reduce(ReduceFunction<T> function) {
		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction can not be a RichFunction.");
		}

		TypeSerializer<T> serializer = dataSet.getType().createSerializer(getExecutionConfig());
		TypeSerializer<K> keySerializer = keyType.createSerializer(getExecutionConfig());

		ReducingStateDescriptor<T> descriptor =
			new ReducingStateDescriptor<>(STATE_DESCRIPTOR_NAME, function, serializer);

		OneInputStreamOperator<T, T> operator =
			new WindowOperator<>(
				windowAssigner,
				windowAssigner.getWindowSerializer(getExecutionConfig()),
				keySelector,
				keySerializer,
				descriptor,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<>()),
				windowAssigner.getDefaultTrigger(null),
				ALLOWED_LATENESS,
				late_data_output_tag());

		return transform(operator);
	}

	/**
	 * Applies the given fold function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the fold function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * @param initialValue An initial accumulator element.
	 * @param function The fold function.
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 * @deprecated use {@link #aggregate(AggregationFunction)} instead
	 */
	@Deprecated
	public <R> Operator fold(R initialValue, FoldFunction<T, R> function) {
		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction can not be a RichFunction.");
		}

		TypeInformation<R> resultType =
			TypeExtractor.getFoldReturnTypes(
				function, dataSet.getType(), Utils.getCallLocationName(), true);

		return fold(initialValue, function, resultType);
	}

	/**
	 * Applies the given fold function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the fold function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * @param initialValue An initial accumulator element.
	 * @param function The fold function.
	 * @param resultType The output type of the fold function.
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 * @deprecated use {@link #aggregate(AggregationFunction)} instead
	 */
	@Deprecated
	public <R> Operator fold(
		R initialValue, FoldFunction<T, R> function, TypeInformation<R> resultType) {
		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of reduce can not be a RichFunction.");
		}

		TypeSerializer<K> keySerializer = keyType.createSerializer(getExecutionConfig());

		FoldingStateDescriptor<T, R> descriptor =
			new FoldingStateDescriptor<>(STATE_DESCRIPTOR_NAME, initialValue, function, resultType);
		descriptor.initializeSerializerUnlessSet(getExecutionConfig());

		OneInputStreamOperator<T, R> operator =
			new WindowOperator<>(
				windowAssigner,
				windowAssigner.getWindowSerializer(getExecutionConfig()),
				keySelector,
				keySerializer,
				descriptor,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<>()),
				windowAssigner.getDefaultTrigger(null),
				ALLOWED_LATENESS,
				late_data_output_tag());

		return transform(operator);
	}

	/**
	 * Applies the given aggregation function to each window. The aggregation function is called for
	 * each element, aggregating values incrementally and keeping the state to one accumulator per key
	 * and window.
	 *
	 * @param function The aggregation function.
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 * @param <ACC> The type of the AggregateFunction's accumulator
	 * @param <R> The type of the elements in the resulting stream, equal to the AggregateFunction's
	 *     result type
	 */
	public <ACC, R> Operator aggregate(AggregateFunction<T, ACC, R> function) {
		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("Aggregate of reduce can not be a RichFunction.");
		}

		TypeInformation<ACC> accumulatorType =
			TypeExtractor.getAggregateFunctionAccumulatorType(function, dataSet.getType(), null, false);

		return aggregate(function, accumulatorType);
	}

	/**
	 * Applies the given aggregation function to each window. The aggregation function is called for
	 * each element, aggregating values incrementally and keeping the state to one accumulator per key
	 * and window.
	 *
	 * @param function The aggregation function.
	 * @param accumulatorType The type of the accumulator.
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 * @param <ACC> The type of the AggregateFunction's accumulator
	 * @param <R> The type of the elements in the resulting stream, equal to the AggregateFunction's
	 *     result type
	 */
	public <ACC, R> Operator aggregate(
		AggregateFunction<T, ACC, R> function, TypeInformation<ACC> accumulatorType) {

		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("Aggregate of reduce can not be a RichFunction.");
		}

		TypeSerializer<K> keySerializer = keyType.createSerializer(getExecutionConfig());

		AggregatingStateDescriptor<T, ACC, R> descriptor = new AggregatingStateDescriptor<>(STATE_DESCRIPTOR_NAME, function, accumulatorType);
		descriptor.initializeSerializerUnlessSet(getExecutionConfig());

		OneInputStreamOperator<T, R> operator =
			new WindowOperator<>(
				windowAssigner,
				windowAssigner.getWindowSerializer(getExecutionConfig()),
				keySelector,
				keySerializer,
				descriptor,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<>()),
				windowAssigner.getDefaultTrigger(null),
				ALLOWED_LATENESS,
				late_data_output_tag());

		return transform(operator);
	}

	/**
	 * Creates a window that uses an {@link Evictor} to evict elements before emission.
	 *
	 * @param evictor The evictor used.
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator evictor(Evictor<? super T, ? super W> evictor) {
		TypeSerializer<T> serializer = dataSet.getType().createSerializer(getExecutionConfig());
		TypeSerializer<K> keySerializer = keyType.createSerializer(getExecutionConfig());

		@SuppressWarnings({"unchecked", "rawtypes"})
		TypeSerializer<StreamRecord<T>> streamRecordSerializer =
			(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(serializer);
		ListStateDescriptor<StreamRecord<T>> stateDesc = new ListStateDescriptor<>(STATE_DESCRIPTOR_NAME, streamRecordSerializer);

		OneInputStreamOperator<T, T> operator =
			new EvictingWindowOperator<>(
				windowAssigner,
				windowAssigner.getWindowSerializer(getExecutionConfig()),
				keySelector,
				keySerializer,
				stateDesc,
				new InternalIterableWindowFunction<>(
					new ReduceApplyWindowFunction<>(
						VoidReduceFunction.instance(),
						new PassThroughWindowFunction<>())),
				windowAssigner.getDefaultTrigger(null),
				evictor,
				ALLOWED_LATENESS,
				late_data_output_tag());

		return transform(operator);
	}

	/**
	 * Collects all elements for each window for operations that do not want or allow pre-aggregations
	 * such as {@code
	 * org.apache.flink.streaming.api.datastream.WindowedStream#process(ProcessWindowFunction)} or
	 * {@code org.apache.flink.streaming.api.datastream.WindowedStream#apply(WindowFunction)}.
	 *
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator collect() {
		ListStateDescriptor<T> descriptor =
			new ListStateDescriptor<>(STATE_DESCRIPTOR_NAME, dataSet.getType());
		descriptor.initializeSerializerUnlessSet(getExecutionConfig());

		TypeSerializer<K> keySerializer = keyType.createSerializer(getExecutionConfig());

		InternalWindowFunction<Iterable<T>, T, K, W> function =
			new InternalIterableProcessWindowFunction<>(VoidProcessWindowFunction.instance());

		WindowOperator<K, T, Iterable<T>, T, W> operator =
			new WindowOperator<>(
				windowAssigner,
				windowAssigner.getWindowSerializer(getExecutionConfig()),
				keySelector,
				keySerializer,
				descriptor,
				function,
				windowAssigner.getDefaultTrigger(null),
				ALLOWED_LATENESS,
				late_data_output_tag());

		StreamConfig config = new BoundedStreamConfig(keySerializer, keySelector);
		config.setStreamOperator(operator);

		return new Operator.OneInput<>(dataSet, config, keySelector, timestampAssigner);
	}

	// ------------------------------------------------------------------------
	//  Pre-defined aggregations on the keyed windows
	// ------------------------------------------------------------------------

	/**
	 * Applies an aggregation that sums every window of the operator at the given position.
	 *
	 * @param positionToSum The position in the tuple/array to sum
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator sum(int positionToSum) {
		return aggregate(new SumAggregator<>(positionToSum, dataSet.getType(), getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that sums every window of the pojo data stream at the given field for
	 * every window.
	 *
	 * <p>A field expression is either the name of a public field or a getter method with parentheses
	 * of the stream's underlying type. A dot can be used to drill down into objects, as in {@code
	 * "field1.getInnerField2()" }.
	 *
	 * @param field The field to sum
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator sum(String field) {
		return aggregate(new SumAggregator<>(field, dataSet.getType(), getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the minimum value of every window of the data stream at
	 * the given position.
	 *
	 * @param positionToMin The position to minimize
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator min(int positionToMin) {
		return aggregate(
			new ComparableAggregator<>(
				positionToMin,
				dataSet.getType(),
				AggregationFunction.AggregationType.MIN,
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the minimum value of the pojo data stream at the given
	 * field expression for every window.
	 *
	 * <p>A field * expression is either the name of a public field or a getter method with
	 * parentheses of the {@link Operator}S underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.getInnerField2()" }.
	 *
	 * @param field The field expression based on which the aggregation will be applied.
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator min(String field) {
		return aggregate(
			new ComparableAggregator<>(
				field,
				dataSet.getType(),
				AggregationFunction.AggregationType.MIN,
				false,
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the minimum element of every window of the data stream by the
	 * given position. If more elements have the same minimum value the operator returns the first
	 * element by default.
	 *
	 * @param positionToMinBy The position to minimize by
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator minBy(int positionToMinBy) {
		return this.minBy(positionToMinBy, true);
	}

	/**
	 * Applies an aggregation that gives the minimum element of every window of the data stream by the
	 * given field. If more elements have the same minimum value the operator returns the first
	 * element by default.
	 *
	 * @param field The field to minimize by
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator minBy(String field) {
		return this.minBy(field, true);
	}

	/**
	 * Applies an aggregation that gives the minimum element of every window of the data stream by the
	 * given position. If more elements have the same minimum value the operator returns either the
	 * first or last one depending on the parameter setting.
	 *
	 * @param positionToMinBy The position to minimize
	 * @param first If true, then the operator return the first element with the minimum value,
	 *     otherwise returns the last
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator minBy(int positionToMinBy, boolean first) {
		return aggregate(
			new ComparableAggregator<>(
				positionToMinBy,
				dataSet.getType(),
				AggregationFunction.AggregationType.MINBY,
				first,
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the minimum element of the pojo data stream by the given
	 * field expression for every window. A field expression is either the name of a public field or a
	 * getter method with parentheses of the {@link Operator}'s underlying type. A dot can be used to
	 * drill down into objects, as in {@code "field1.getInnerField2()" }.
	 *
	 * @param field The field expression based on which the aggregation will be applied.
	 * @param first If True then in case of field equality the first object will be returned
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator minBy(String field, boolean first) {
		return aggregate(
			new ComparableAggregator<>(
				field,
				dataSet.getType(),
				AggregationFunction.AggregationType.MINBY,
				first,
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the maximum value of every window of the data stream at the
	 * given position.
	 *
	 * @param positionToMax The position to maximize
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator max(int positionToMax) {
		return aggregate(
			new ComparableAggregator<>(
				positionToMax,
				dataSet.getType(),
				AggregationFunction.AggregationType.MAX,
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the maximum value of the pojo data stream at the given
	 * field expression for every window. A field expression is either the name of a public field or a
	 * getter method with parentheses of the {@link Operator}'s underlying type. A dot can be used to
	 * drill down into objects, as in {@code "field1.getInnerField2()" }.
	 *
	 * @param field The field expression based on which the aggregation will be applied.
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator max(String field) {
		return aggregate(
			new ComparableAggregator<>(
				field,
				dataSet.getType(),
				AggregationFunction.AggregationType.MAX,
				false,
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the maximum element of every window of the data stream by the
	 * given position. If more elements have the same maximum value the operator returns the first by
	 * default.
	 *
	 * @param positionToMaxBy The position to maximize by
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator maxBy(int positionToMaxBy) {
		return this.maxBy(positionToMaxBy, true);
	}

	/**
	 * Applies an aggregation that gives the maximum element of every window of the data stream by the
	 * given field. If more elements have the same maximum value the operator returns the first by
	 * default.
	 *
	 * @param field The field to maximize by
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator maxBy(String field) {
		return this.maxBy(field, true);
	}

	/**
	 * Applies an aggregation that gives the maximum element of every window of the data stream by the
	 * given position. If more elements have the same maximum value the operator returns either the
	 * first or last one depending on the parameter setting.
	 *
	 * @param positionToMaxBy The position to maximize by
	 * @param first If true, then the operator return the first element with the maximum value,
	 *     otherwise returns the last
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator maxBy(int positionToMaxBy, boolean first) {
		return aggregate(
			new ComparableAggregator<>(
				positionToMaxBy,
				dataSet.getType(),
				AggregationFunction.AggregationType.MAXBY,
				first,
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the maximum element of the pojo data stream by the given
	 * field expression for every window. A field expression is either the name of a public field or a
	 * getter method with parentheses of the {@link Operator}'s underlying type. A dot can be used to
	 * drill down into objects, as in {@code "field1.getInnerField2()" }.
	 *
	 * @param field The field expression based on which the aggregation will be applied.
	 * @param first If True then in case of field equality the first object will be returned
	 * @return An {@link Operator} that can be added to a {@link Savepoint}.
	 */
	public Operator maxBy(String field, boolean first) {
		return aggregate(
			new ComparableAggregator<>(
				field,
				dataSet.getType(),
				AggregationFunction.AggregationType.MAXBY,
				first,
				getExecutionConfig()));
	}

	private Operator aggregate(AggregationFunction<T> aggregator) {
		ReducingStateDescriptor<T> descriptor =
			new ReducingStateDescriptor<>(STATE_DESCRIPTOR_NAME, aggregator, dataSet.getType());
		descriptor.initializeSerializerUnlessSet(getExecutionConfig());

		TypeSerializer<K> keySerializer = keyType.createSerializer(getExecutionConfig());

		WindowOperator<K, T, T, T, W> operator =
			new WindowOperator<>(
				windowAssigner,
				windowAssigner.getWindowSerializer(getExecutionConfig()),
				keySelector,
				keySerializer,
				descriptor,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<>()),
				windowAssigner.getDefaultTrigger(null),
				ALLOWED_LATENESS,
				late_data_output_tag());

		return transform(operator);
	}

	private static class VoidReduceFunction<T> implements ReduceFunction<T> {
		static <T> VoidReduceFunction<T> instance() {
			return new VoidReduceFunction<>();
		}

		private VoidReduceFunction() {}

		@Override
		public T reduce(T value1, T value2) {
			return null;
		}
	}

	private static class VoidProcessWindowFunction<IN, OUT, KEY, W extends Window> extends ProcessWindowFunction<IN, OUT, KEY, W> {
		static <IN, OUT, KEY, W extends Window> VoidProcessWindowFunction<IN, OUT, KEY, W> instance() {
			return new VoidProcessWindowFunction<>();
		}

		private VoidProcessWindowFunction() {}

		@Override
		public void process(KEY key, Context context, Iterable<IN> elements, Collector<OUT> out) {}
	}

	private ExecutionConfig getExecutionConfig() {
		return dataSet.getExecutionEnvironment().getConfig();
	}

	private Operator transform(OneInputStreamOperator<T, ?> operator) {
		TypeSerializer<K> keySerializer =
			keyType.createSerializer(dataSet.getExecutionEnvironment().getConfig());

		StreamConfig config = new BoundedStreamConfig(keySerializer, keySelector);
		config.setStreamOperator(operator);

		return new Operator.OneInput<>(dataSet, config, keySelector, timestampAssigner);
	}
}

