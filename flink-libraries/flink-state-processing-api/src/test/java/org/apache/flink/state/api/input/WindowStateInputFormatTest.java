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

package org.apache.flink.state.api.input;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.functions.WindowStateReaderFunction;
import org.apache.flink.state.api.input.splits.KeyGroupRangeInputSplit;
import org.apache.flink.state.api.output.windowing.InternalSingleValueWindowStateReaderFunction;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests the basic functionality of the {@code WindowStateInputFormat}.
 */
public class WindowStateInputFormatTest {

	@Test
	public void testPreAggWindowReader() throws Exception {
		ReducingStateDescriptor<Integer> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer(),
			Types.INT.createSerializer(new ExecutionConfig()));

		WindowOperator<Integer, Integer, Integer, Integer, TimeWindow> operator = new WindowOperator<>(
			TumblingEventTimeWindows.of(Time.of(10, TimeUnit.MILLISECONDS)),
			new TimeWindow.Serializer(),
			new IdentityKeySelector(),
			Types.INT.createSerializer(new ExecutionConfig()),
			stateDesc,
			new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<>()),
			EventTimeTrigger.create(),
			0,
			null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Integer, Integer> testHarness = createTestHarness(operator);
		testHarness.processElement(new StreamRecord<>(1, 0));
		testHarness.processElement(new StreamRecord<>(2, 0));
		testHarness.processElement(new StreamRecord<>(3, 0));

		OperatorSubtaskState state = testHarness.snapshot(0, 0);

		OperatorID operatorID = OperatorIDGenerator.fromUid("uid");
		OperatorState operatorState = new OperatorState(operatorID, 1, 128);
		operatorState.putState(0, state);

		WindowStateInputFormat<Integer, Integer, Tuple3<Integer, Integer, TimeWindow>, Integer, TimeWindow> format =
			new WindowStateInputFormat<>(operatorState, new MemoryStateBackend(), Types.INT, new InternalSingleValueWindowStateReaderFunction<>(new UserReaderFunction()), new TimeWindow.Serializer(), stateDesc);

		KeyGroupRangeInputSplit split = format.createInputSplits(1)[0];

		List<Tuple3<Integer, Integer, TimeWindow>> data = readInputSplit(split, format);
		Assert.assertEquals(3, data.size());
	}

	private static class IdentityKeySelector implements KeySelector<Integer, Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public Integer getKey(Integer value) {
			return value;
		}
	}

	private static class SumReducer implements ReduceFunction<Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public Integer reduce(Integer value1, Integer value2) {
			return value1 + value2;
		}
	}

	private static OneInputStreamOperatorTestHarness<Integer, Integer> createTestHarness(OneInputStreamOperator<Integer, Integer> operator) throws Exception {
		OneInputStreamOperatorTestHarness<Integer, Integer> harness =  new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector(), Types.INT);
		harness.setup(Types.INT.createSerializer(new ExecutionConfig()));
		harness.open();
		return harness;
	}

	@Nonnull
	private List<Tuple3<Integer, Integer, TimeWindow>> readInputSplit(KeyGroupRangeInputSplit split, WindowStateInputFormat<Integer, Integer, Tuple3<Integer, Integer, TimeWindow>, Integer, TimeWindow> format) throws IOException {
		List<Tuple3<Integer, Integer, TimeWindow>> data = new ArrayList<>();

		format.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));

		format.openInputFormat();
		format.open(split);

		while (!format.reachedEnd()) {
			data.add(format.nextRecord(null));
		}

		format.close();
		format.closeInputFormat();

		return data;
	}

	private static class UserReaderFunction extends WindowStateReaderFunction<Integer, Tuple3<Integer, Integer, TimeWindow>, Integer, TimeWindow> {

		@Override
		public void readKey(Integer key, Context context, Iterable<Integer> elements, Collector<Tuple3<Integer, Integer, TimeWindow>> out) {
			Iterator<Integer> iterator = elements.iterator();
			Integer value = iterator.next();

			out.collect(Tuple3.of(key, value, context.window()));

			Assert.assertTrue(!iterator.hasNext());
		}
	}
}
