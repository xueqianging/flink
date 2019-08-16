/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.streaming.api.functions.windowing.AggregateApplyAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.FoldApplyAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.FoldApplyProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.BaseAlignedWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalAggregateProcessAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableProcessAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueProcessAllWindowFunction;
import org.apache.flink.util.Preconditions;

/**
 * A builder for creating {@code WindowOperator}'s.
 * @param <T> The type of the incoming elements.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
public class AllWindowOperatorBuilder<T, W extends Window>  extends WindowOperatorBuilderBase<T, Byte, W> {

	public AllWindowOperatorBuilder(
		WindowAssigner<? super T, W> windowAssigner,
		Trigger<? super T, ? super W> trigger,
		ExecutionConfig config,
		TypeInformation<T> inputType) {
		super(windowAssigner, trigger, config, inputType, new NullByteKeySelector<>(), Types.BYTE);
	}

	public <R> WindowOperator<Byte, T, ?, R, W> reduce(ReduceFunction<T> reduceFunction, AllWindowFunction<T, R, W> function) {
		Preconditions.checkNotNull(reduceFunction, "ReduceFunction cannot be null");
		Preconditions.checkNotNull(function, "WindowFunction cannot be null");

		if (reduceFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of apply can not be a RichFunction.");
		}

		if (evictor != null) {
			return buildEvictingWindowOperator(new InternalIterableAllWindowFunction<>(new ReduceApplyAllWindowFunction<>(reduceFunction, function)));
		} else {
			ReducingStateDescriptor<T> stateDesc = new ReducingStateDescriptor<>(WINDOW_STATE_NAME, reduceFunction, inputType.createSerializer(config));

			return buildWindowOperator(stateDesc, new InternalSingleValueAllWindowFunction<>(function));
		}
	}

	public <R> WindowOperator<Byte, T, ?, R, W> reduce(ReduceFunction<T> reduceFunction, ProcessAllWindowFunction<T, R, W> function) {
		Preconditions.checkNotNull(reduceFunction, "ReduceFunction cannot be null");
		Preconditions.checkNotNull(function, "ProcessWindowFunction cannot be null");

		if (reduceFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of apply can not be a RichFunction.");
		}

		if (evictor != null) {
			return buildEvictingWindowOperator(new InternalIterableProcessAllWindowFunction<>(new ReduceApplyProcessAllWindowFunction<>(reduceFunction, function)));
		} else {
			ReducingStateDescriptor<T> stateDesc = new ReducingStateDescriptor<>(WINDOW_STATE_NAME, reduceFunction, inputType.createSerializer(config));

			return buildWindowOperator(stateDesc, new InternalSingleValueProcessAllWindowFunction<>(function));
		}
	}

	@Deprecated
	public <ACC, R> WindowOperator<Byte, T, ?, R, W> fold(
		ACC initialValue,
		FoldFunction<T, ACC> foldFunction,
		AllWindowFunction<ACC, R, W> function,
		TypeInformation<ACC> foldAccumulatorType) {

		Preconditions.checkNotNull(foldAccumulatorType, "FoldFunction cannot be null");
		Preconditions.checkNotNull(function, "WindowFunction cannot be null");

		if (foldFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of fold can not be a RichFunction.");
		}
		if (windowAssigner instanceof MergingWindowAssigner) {
			throw new UnsupportedOperationException("Fold cannot be used with a merging WindowAssigner.");
		}

		if (windowAssigner instanceof BaseAlignedWindowAssigner) {
			throw new UnsupportedOperationException("Fold cannot be used with a " +
				windowAssigner.getClass().getSimpleName() + " assigner.");
		}

		if (evictor != null) {
			return buildEvictingWindowOperator(new InternalIterableAllWindowFunction<>(new FoldApplyAllWindowFunction<>(initialValue, foldFunction, function, foldAccumulatorType)));
		} else {
			FoldingStateDescriptor<T, ACC> stateDesc = new FoldingStateDescriptor<>(WINDOW_STATE_NAME,
				initialValue, foldFunction, foldAccumulatorType.createSerializer(config));
			return buildWindowOperator(stateDesc, new InternalSingleValueAllWindowFunction<>(function));
		}
	}

	@Deprecated
	public <ACC, R> WindowOperator<Byte, T, ?, R, W> fold(
		ACC initialValue,
		FoldFunction<T, ACC> foldFunction,
		ProcessAllWindowFunction<ACC, R, W> windowFunction,
		TypeInformation<ACC> foldAccumulatorType) {

		Preconditions.checkNotNull(foldAccumulatorType, "FoldFunction cannot be null");
		Preconditions.checkNotNull(windowFunction, "ProcessWindowFunction cannot be null");

		if (foldFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of fold can not be a RichFunction.");
		}
		if (windowAssigner instanceof MergingWindowAssigner) {
			throw new UnsupportedOperationException("Fold cannot be used with a merging WindowAssigner.");
		}

		if (windowAssigner instanceof BaseAlignedWindowAssigner) {
			throw new UnsupportedOperationException("Fold cannot be used with a " +
				windowAssigner.getClass().getSimpleName() + " assigner.");
		}

		if (evictor != null) {
			InternalIterableProcessAllWindowFunction<T, R, W> internalFunction = new InternalIterableProcessAllWindowFunction<>(
				new FoldApplyProcessAllWindowFunction<>(initialValue, foldFunction, windowFunction, foldAccumulatorType));
			return buildEvictingWindowOperator(internalFunction);
		} else {
			FoldingStateDescriptor<T, ACC> stateDesc = new FoldingStateDescriptor<>(WINDOW_STATE_NAME,
				initialValue, foldFunction, foldAccumulatorType.createSerializer(config));
			return buildWindowOperator(stateDesc, new InternalSingleValueProcessAllWindowFunction<>(windowFunction));
		}
	}

	public <ACC, V, R> WindowOperator<Byte, T, ?, R, W> aggregate(
		AggregateFunction<T, ACC, V> aggregateFunction,
		AllWindowFunction<V, R, W> windowFunction,
		TypeInformation<ACC> accumulatorType) {

		Preconditions.checkNotNull(aggregateFunction, "AggregateFunction cannot be null");
		Preconditions.checkNotNull(windowFunction, "WindowFunction cannot be null");

		if (aggregateFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("This aggregate function cannot be a RichFunction.");
		}

		if (evictor != null) {
			return buildEvictingWindowOperator(new InternalIterableAllWindowFunction<>(new AggregateApplyAllWindowFunction<>(aggregateFunction, windowFunction)));
		} else {
			AggregatingStateDescriptor<T, ACC, V> stateDesc = new AggregatingStateDescriptor<>(WINDOW_STATE_NAME,
				aggregateFunction, accumulatorType.createSerializer(config));

			return buildWindowOperator(stateDesc, new InternalSingleValueAllWindowFunction<>(windowFunction));
		}
	}

	public <ACC, V, R> WindowOperator<Byte, T, ?, R, W> aggregate(
		AggregateFunction<T, ACC, V> aggregateFunction,
		ProcessAllWindowFunction<V, R, W> windowFunction,
		TypeInformation<ACC> accumulatorType) {

		Preconditions.checkNotNull(aggregateFunction, "AggregateFunction cannot be null");
		Preconditions.checkNotNull(windowFunction, "ProcessWindowFunction cannot be null");

		if (aggregateFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("This aggregate function cannot be a RichFunction.");
		}

		if (evictor != null) {
			return buildEvictingWindowOperator(new InternalAggregateProcessAllWindowFunction<>(aggregateFunction, windowFunction));
		} else {
			AggregatingStateDescriptor<T, ACC, V> stateDesc = new AggregatingStateDescriptor<>(WINDOW_STATE_NAME,
				aggregateFunction, accumulatorType.createSerializer(config));

			return buildWindowOperator(stateDesc, new InternalSingleValueProcessAllWindowFunction<>(windowFunction));
		}
	}

	public <R> WindowOperator<Byte, T, ?, R, W> apply(AllWindowFunction<T, R, W> function) {
		Preconditions.checkNotNull(function, "WindowFunction cannot be null");
		return apply(new InternalIterableAllWindowFunction<>(function));
	}

	public <R> WindowOperator<Byte, T, ?, R, W> process(ProcessAllWindowFunction<T, R, W> function) {
		Preconditions.checkNotNull(function, "ProcessWindowfunction cannot be null");
		return apply(new InternalIterableProcessAllWindowFunction<>(function));
	}
}
