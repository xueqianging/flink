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

package org.apache.flink.state.api;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * IT case for reading state.
 */
@RunWith(Parameterized.class)
public class SavepointReaderKeyedStateITCase extends SavepointTestBase {
	private static final String uid = "stateful-operator";

	private static ValueStateDescriptor<Integer> valueState = new ValueStateDescriptor<>("value", Types.INT);

	private final StateBackend backend;

	@Parameterized.Parameters(name = "backend = {0}")
	public static Object[] data() {
		return new Object[][] {
			{ "memory", new MemoryStateBackend() },
			{ "rocksdb", new RocksDBStateBackend((StateBackend) new MemoryStateBackend())}
		};
	}

	private static final List<Pojo> elements = Arrays.asList(
		Pojo.of(1, 1),
		Pojo.of(2, 2),
		Pojo.of(3, 3));

	private static final List<Integer> numbers = Arrays.asList(1, 2, 3);

	@SuppressWarnings("unused")
	public SavepointReaderKeyedStateITCase(String name, StateBackend backend) {
		this.backend = backend;
	}

	@Test
	public void testUserKeyedStateReader() throws Exception {
		String savepointPath = takeSavepoint(elements, source -> {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setStateBackend(backend);
			env.setParallelism(4);

			env
				.addSource(source)
				.rebalance()
				.keyBy(id -> id.key)
				.process(new KeyedStatefulOperator())
				.uid(uid)
				.addSink(new DiscardingSink<>());

			return env;
		});

		ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
		ExistingSavepoint savepoint = Savepoint.load(batchEnv, savepointPath, backend);

		List<Pojo> results = savepoint
			.readKeyedState(uid, new Reader())
			.collect();

		Set<Pojo> expected = new HashSet<>(elements);

		Assert.assertEquals("Unexpected results from keyed state", expected, new HashSet<>(results));
	}

	@Test
	public void testReduceWindowStateReader() throws Exception {
		String savepointPath = takeSavepoint(numbers, source -> {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setStateBackend(backend);
			env.setParallelism(4);

			env
				.addSource(source)
				.rebalance()
				.assignTimestampsAndWatermarks(new ZeroTimestampAssigner<>())
				.keyBy(id -> id)
				.timeWindow(Time.milliseconds(10))
				.reduce((a, b) -> a + b)
				.uid(uid)
				.addSink(new DiscardingSink<>());

			return env;
		});

		ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
		ExistingSavepoint savepoint = Savepoint.load(batchEnv, savepointPath, backend);

		List<Integer> results = savepoint
			.timeWindow(uid)
			.reduce((a, b) -> a + b, Types.INT, Types.INT)
			.collect();

		Set<Integer> expected = new HashSet<>(numbers);
		Assert.assertEquals("Unexpected results from keyed state", expected, new HashSet<>(results));
	}

	private static class KeyedStatefulOperator extends KeyedProcessFunction<Integer, Pojo, Void> {

		private transient ValueState<Integer> state;

		@Override
		public void open(Configuration parameters) {
			state = getRuntimeContext().getState(valueState);
		}

		@Override
		public void processElement(Pojo value, Context ctx, Collector<Void> out) throws Exception {
			state.update(value.state);

			value.eventTimeTimer.forEach(timer -> ctx.timerService().registerEventTimeTimer(timer));
			value.processingTimeTimer.forEach(timer -> ctx.timerService().registerProcessingTimeTimer(timer));
		}
	}

	private static class Reader extends KeyedStateReaderFunction<Integer, Pojo> {

		private transient ValueState<Integer> state;

		@Override
		public void open(Configuration parameters) {
			state = getRuntimeContext().getState(valueState);
		}

		@Override
		public void readKey(Integer key, Context ctx, Collector<Pojo> out) throws Exception {
			Pojo pojo = new Pojo();
			pojo.key = key;
			pojo.state = state.value();
			pojo.eventTimeTimer = ctx.registeredEventTimeTimers();
			pojo.processingTimeTimer = ctx.registeredProcessingTimeTimers();

			out.collect(pojo);
		}
	}

	/**
	 * A simple pojo type.
	 */
	public static class Pojo {
		public static Pojo of(Integer key, Integer state) {
			Pojo wrapper = new Pojo();
			wrapper.key = key;
			wrapper.state = state;
			wrapper.eventTimeTimer = Collections.singleton(Long.MAX_VALUE - 1);
			wrapper.processingTimeTimer = Collections.singleton(Long.MAX_VALUE - 2);

			return wrapper;
		}

		Integer key;

		Integer state;

		Set<Long> eventTimeTimer;

		Set<Long> processingTimeTimer;

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			} else if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Pojo pojo = (Pojo) o;
			return Objects.equals(key, pojo.key) &&
				Objects.equals(state, pojo.state) &&
				Objects.equals(eventTimeTimer, pojo.eventTimeTimer) &&
				Objects.equals(processingTimeTimer, pojo.processingTimeTimer);
		}

		@Override
		public int hashCode() {
			return Objects.hash(key, state, eventTimeTimer, processingTimeTimer);
		}
	}

	private static class ZeroTimestampAssigner<T> implements AssignerWithPunctuatedWatermarks<T> {
		@Nullable
		@Override
		public Watermark checkAndGetNextWatermark(T lastElement, long extractedTimestamp) {
			return null;
		}

		@Override
		public long extractTimestamp(T element, long previousElementTimestamp) {
			return 0;
		}
	}
}
