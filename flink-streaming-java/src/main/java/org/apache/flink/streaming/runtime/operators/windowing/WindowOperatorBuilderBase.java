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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.assigners.BaseAlignedWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.lang.reflect.Type;

/**
 * A builder for creating {@code WindowOperator}'s.
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <T> The type of the incoming elements.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
class WindowOperatorBuilderBase<T, K, W extends Window> {

	public static final String WINDOW_STATE_NAME = "window-contents";

	protected final ExecutionConfig config;

	protected final WindowAssigner<? super T, W> windowAssigner;

	protected final TypeInformation<T> inputType;

	protected final KeySelector<T, K> keySelector;

	protected final TypeInformation<K> keyType;

	protected Trigger<? super T, ? super W> trigger;

	@Nullable
	protected Evictor<? super T, ? super W> evictor;

	protected long allowedLateness = 0L;

	@Nullable
	private OutputTag<T> lateDataOutputTag;

	WindowOperatorBuilderBase(
		WindowAssigner<? super T, W> windowAssigner,
		Trigger<? super T, ? super W> trigger,
		ExecutionConfig config,
		TypeInformation<T> inputType,
		KeySelector<T, K> keySelector,
		TypeInformation<K> keyType) {
		this.windowAssigner = windowAssigner;
		this.config = config;
		this.inputType = inputType;
		this.keySelector = keySelector;
		this.keyType = keyType;
		this.trigger = trigger;
	}

	public void trigger(Trigger<? super T, ? super W> trigger) {
		Preconditions.checkNotNull(trigger, "Window triggers cannot be null");

		if (windowAssigner instanceof MergingWindowAssigner && !trigger.canMerge()) {
			throw new UnsupportedOperationException("A merging window assigner cannot be used with a trigger that does not support merging.");
		}

		if (windowAssigner instanceof BaseAlignedWindowAssigner) {
			throw new UnsupportedOperationException("Cannot use a " + windowAssigner.getClass().getSimpleName() + " with a custom trigger.");
		}

		this.trigger = trigger;
	}

	public void allowedLateness(Time lateness) {
		Preconditions.checkNotNull(lateness, "Allowed lateness cannot be null");

		final long millis = lateness.toMilliseconds();
		Preconditions.checkArgument(millis >= 0, "The allowed lateness cannot be negative.");

		this.allowedLateness = millis;
	}

	public void sideOutputLateData(OutputTag<T> outputTag) {
		Preconditions.checkNotNull(outputTag, "Side output tag must not be null.");
		this.lateDataOutputTag = outputTag;
	}

	public void evictor(Evictor<? super T, ? super W> evictor) {
		Preconditions.checkNotNull(evictor, "Evictor cannot be null");

		if (windowAssigner instanceof BaseAlignedWindowAssigner) {
			throw new UnsupportedOperationException("Cannot use a " + windowAssigner.getClass().getSimpleName() + " with an Evictor.");
		}
		this.evictor = evictor;
	}

	protected <R> WindowOperator<K, T, ?, R, W> apply(InternalWindowFunction<Iterable<T>, R, K, W> function) {
		if (evictor != null) {
			return buildEvictingWindowOperator(function);
		} else {
			ListStateDescriptor<T> stateDesc = new ListStateDescriptor<>(WINDOW_STATE_NAME, inputType.createSerializer(config));

			return buildWindowOperator(stateDesc, function);
		}
	}

	protected  <ACC, R> WindowOperator<K, T, ACC, R, W> buildWindowOperator(
		StateDescriptor<? extends AppendingState<T, ACC>, ?> stateDesc,
		InternalWindowFunction<ACC, R, K, W> function) {

		return new WindowOperator<>(
			windowAssigner,
			windowAssigner.getWindowSerializer(config),
			keySelector,
			keyType.createSerializer(config),
			stateDesc,
			function,
			trigger,
			allowedLateness,
			lateDataOutputTag);
	}

	protected  <R> WindowOperator<K, T, Iterable<T>, R, W> buildEvictingWindowOperator(InternalWindowFunction<Iterable<T>, R, K, W> function) {
		@SuppressWarnings({"unchecked", "rawtypes"})
		TypeSerializer<StreamRecord<T>> streamRecordSerializer =
			(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(inputType.createSerializer(config));

		ListStateDescriptor<StreamRecord<T>> stateDesc = new ListStateDescriptor<>(WINDOW_STATE_NAME, streamRecordSerializer);

		return new EvictingWindowOperator<>(windowAssigner,
			windowAssigner.getWindowSerializer(config),
			keySelector,
			keyType.createSerializer(config),
			stateDesc,
			function,
			trigger,
			evictor,
			allowedLateness,
			lateDataOutputTag);
	}

	private static String generateFunctionName(Function function) {
		Class<? extends Function> functionClass = function.getClass();
		if (functionClass.isAnonymousClass()) {
			// getSimpleName returns an empty String for anonymous classes
			Type[] interfaces = functionClass.getInterfaces();
			if (interfaces.length == 0) {
				// extends an existing class (like RichMapFunction)
				Class<?> functionSuperClass = functionClass.getSuperclass();
				return functionSuperClass.getSimpleName() + functionClass.getName().substring(functionClass.getEnclosingClass().getName().length());
			} else {
				// implements a Function interface
				Class<?> functionInterface = functionClass.getInterfaces()[0];
				return functionInterface.getSimpleName() + functionClass.getName().substring(functionClass.getEnclosingClass().getName().length());
			}
		} else {
			return functionClass.getSimpleName();
		}
	}

	public String generateOperatorName(Function function1, @Nullable Function function2) {
		return "Window(" +
			windowAssigner + ", " +
			trigger.getClass().getSimpleName() + ", " +
			(evictor == null ? "" : (evictor.getClass().getSimpleName() + ", ")) +
			generateFunctionName(function1) +
			(function2 == null ? "" : (", " + generateFunctionName(function2))) +
			")";
	}

	@VisibleForTesting
	public long getAllowedLateness() {
		return allowedLateness;
	}
}
