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

package org.apache.flink.connectors.savepoint.input;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.savepoint.functions.ProcessReaderFunction;
import org.apache.flink.connectors.savepoint.input.splits.KeyGroupRangeInputSplit;
import org.apache.flink.connectors.savepoint.runtime.OperatorIDGenerator;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Tests for keyed state input format.
 */
public class KeyedStateInputFormatTest {
	private static ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("state", Types.INT);

	@Test
	public void testCreatePartitionedInputSplits() throws Exception {
		OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

		OperatorSubtaskState state = createOperatorSubtaskState();
		OperatorState operatorState = new OperatorState(operatorID, 1, 128);
		operatorState.putState(0, state);

		KeyGroupRangeInputSplit[] splits = KeyedStateInputFormat.getKeyGroupRangeInputSplits(4, operatorState);
		Assert.assertEquals("Failed to properly partition operator state into input splits", 4, splits.length);
	}

	@Test
	public void testMaxParallelismRespected() throws Exception {
		OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

		OperatorSubtaskState state = createOperatorSubtaskState();
		OperatorState operatorState = new OperatorState(operatorID, 1, 128);
		operatorState.putState(0, state);

		KeyGroupRangeInputSplit[] splits = KeyedStateInputFormat.getKeyGroupRangeInputSplits(400, operatorState);
		Assert.assertEquals("Failed to properly partition operator state into input splits", 128, splits.length);
	}

	@Test
	public void testRestoreKeyRangeInputSplit() throws Exception {
		OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

		OperatorSubtaskState state = createOperatorSubtaskState();
		OperatorState operatorState = new OperatorState(operatorID, 1, 128);
		operatorState.putState(0, state);

		KeyGroupRangeInputSplit split = KeyedStateInputFormat.getKeyGroupRangeInputSplits(1, operatorState)[0];

		AbstractKeyedStateBackend<Integer> backend =
			KeyedStateInputFormat.createAndRestoreKeyedStateBackend(
				new DummyEnvironment(),
				new MemoryStateBackend(),
				IntSerializer.INSTANCE,
				"uid",
				split,
				new CloseableRegistry());

		long numKeys = backend.getKeys(stateDescriptor.getName(), VoidNamespace.INSTANCE).count();
		Assert.assertEquals("Incorrect number of state values restored", 3, numKeys);

		for (int i = 1; i <= 3; i++) {
			backend.setCurrentKey(i);
			ValueState<Integer> stateHandle = backend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE,
				stateDescriptor);

			Assert.assertEquals("Incorrect state value restored", Integer.valueOf(i), stateHandle.value());
		}
	}

	@Test
	public void testReadState() throws Exception {
		OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

		OperatorSubtaskState state = createOperatorSubtaskState();
		OperatorState operatorState = new OperatorState(operatorID, 1, 128);
		operatorState.putState(0, state);

		KeyGroupRangeInputSplit split = KeyedStateInputFormat.getKeyGroupRangeInputSplits(1, operatorState)[0];

		ProcessReaderFunction<Integer, Integer> userFunction = new ReaderFunction();

		List<Integer> data = readInputSplit(split, userFunction);

		Assert.assertEquals("Incorrect data read from input split", Arrays.asList(1, 2, 3), data);
	}

	@Test
	public void testReadMultipleOutputPerKey() throws Exception {
		OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

		OperatorSubtaskState state = createOperatorSubtaskState();
		OperatorState operatorState = new OperatorState(operatorID, 1, 128);
		operatorState.putState(0, state);

		KeyGroupRangeInputSplit split = KeyedStateInputFormat.getKeyGroupRangeInputSplits(1, operatorState)[0];

		ProcessReaderFunction<Integer, Integer> userFunction = new DoubleReaderFunction();

		List<Integer> data = readInputSplit(split, userFunction);

		Assert.assertEquals("Incorrect data read from input split", Arrays.asList(1, 1, 2, 2, 3, 3), data);
	}

	@Test(expected = IOException.class)
	public void testInvalidProcessReaderFunctionFails() throws Exception {
		OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

		OperatorSubtaskState state = createOperatorSubtaskState();
		OperatorState operatorState = new OperatorState(operatorID, 1, 128);
		operatorState.putState(0, state);

		KeyGroupRangeInputSplit split = KeyedStateInputFormat.getKeyGroupRangeInputSplits(1, operatorState)[0];

		ProcessReaderFunction<Integer, Integer> userFunction = new InvalidReaderFunction();

		readInputSplit(split, userFunction);

		Assert.fail("ProcessReaderFunction did not fail on invalid RuntimeContext use");
	}

	@Nonnull
	private List<Integer> readInputSplit(
		KeyGroupRangeInputSplit split, ProcessReaderFunction<Integer, Integer> userFunction)
		throws java.io.IOException {
		KeyedStateInputFormat<Integer, Integer> format =
			new KeyedStateInputFormat<>("", "uid", new MemoryStateBackend(), Types.INT, userFunction);

		List<Integer> data = new ArrayList<>();

		format.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));

		format.openInputFormat();
		format.open(split);

		while (!format.reachedEnd()) {
			data.add(format.nextRecord(0));
		}

		format.close();
		format.closeInputFormat();

		data.sort(Comparator.comparingInt(id -> id));
		return data;
	}

	private OperatorSubtaskState createOperatorSubtaskState() throws Exception {
		StreamFlatMap<Integer, Void> operator = new StreamFlatMap<>(new StatefulFunction());
		try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, id -> id, Types.INT, 128, 1, 0)) {

			testHarness.setup(VoidSerializer.INSTANCE);
			testHarness.open();

			testHarness.processElement(1, 0);
			testHarness.processElement(2, 0);
			testHarness.processElement(3, 0);

			return testHarness.snapshot(0, 0);
		}
	}

	static class ReaderFunction extends ProcessReaderFunction<Integer, Integer> {
		ValueState<Integer> state;

		@Override
		public void open(Configuration parameters) {
			state = getRuntimeContext().getState(stateDescriptor);
		}

		@Override
		public void processKey(Integer key, Collector<Integer> out) throws Exception {
			out.collect(state.value());
		}
	}

	static class DoubleReaderFunction extends ProcessReaderFunction<Integer, Integer> {
		ValueState<Integer> state;

		@Override
		public void open(Configuration parameters) {
			state = getRuntimeContext().getState(stateDescriptor);
		}

		@Override
		public void processKey(Integer key, Collector<Integer> out) throws Exception {
			out.collect(state.value());
			out.collect(state.value());
		}
	}

	static class InvalidReaderFunction extends ProcessReaderFunction<Integer, Integer> {

		@Override
		public void open(Configuration parameters) {
			getRuntimeContext().getState(stateDescriptor);
		}

		@Override
		public void processKey(Integer key, Collector<Integer> out) throws Exception {
			ValueState<Integer> state = getRuntimeContext().getState(stateDescriptor);
			out.collect(state.value());
		}
	}

	static class StatefulFunction extends RichFlatMapFunction<Integer, Void> {
		ValueState<Integer> state;

		@Override
		public void open(Configuration parameters) {
			state = getRuntimeContext().getState(stateDescriptor);
		}

		@Override
		public void flatMap(Integer value, Collector<Void> out) throws Exception {
			state.update(value);
		}
	}
}

