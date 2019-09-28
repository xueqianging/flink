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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * IT case for reading state.
 */
@RunWith(Parameterized.class)
public class SavepointReaderITTest extends SavepointTestBase {
	private static final String UID = "stateful-operator";

	private static final String LIST_NAME = "list";

	private static final String UNION_NAME = "union";

	private static final String BROADCAST_NAME = "broadcast";

	private static final List<Integer> elements = Arrays.asList(1, 2, 3);

	private final Runner runner;

	@Parameterized.Parameters
	public static Object[] data() {
		return new Object[] {
			new DefaultSerializationRunner(),
			new CustomSerializationRunner()
		};
	}

	public SavepointReaderITTest(Runner runner) {
		this.runner = runner;
	}

	@Test
	public void testOperatorStateInputFormat() throws Exception {

		String savepoint = takeSavepoint(elements, source -> {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(4);

			DataStream<Integer> data = env
				.addSource(source)
				.rebalance();

			data
				.connect(data.broadcast(runner.getBroadcastStateDescriptor()))
				.process(new StatefulOperator(
					runner.getListStateDescriptor(),
					runner.getUnionStateDescriptor(),
					runner.getBroadcastStateDescriptor()))
				.uid(UID)
				.addSink(new DiscardingSink<>());

			return env;
		});

		ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();

		verifyListState(savepoint, batchEnv);

		verifyUnionState(savepoint, batchEnv);

		verifyBroadcastState(savepoint, batchEnv);
	}

	private void verifyListState(String path, ExecutionEnvironment batchEnv) throws Exception {
		ExistingSavepoint savepoint = Savepoint.load(batchEnv, path, new MemoryStateBackend());
		List<Integer> listResult = runner.readListState(savepoint).collect();
		listResult.sort(Comparator.naturalOrder());

		Assert.assertEquals("Unexpected elements read from list state", elements, listResult);
	}

	private void verifyUnionState(String path, ExecutionEnvironment batchEnv) throws Exception {
		ExistingSavepoint savepoint = Savepoint.load(batchEnv, path, new MemoryStateBackend());
		List<Integer> unionResult = runner.readUnionState(savepoint).collect();
		unionResult.sort(Comparator.naturalOrder());

		Assert.assertEquals("Unexpected elements read from union state", elements, unionResult);
	}

	private void verifyBroadcastState(String path, ExecutionEnvironment batchEnv) throws Exception {
		ExistingSavepoint savepoint = Savepoint.load(batchEnv, path, new MemoryStateBackend());
		List<Tuple2<Integer, String>> broadcastResult = runner.readBroadcastState(savepoint)
			.collect();

		List<Integer> broadcastStateKeys  = broadcastResult.
			stream()
			.map(entry -> entry.f0)
			.sorted(Comparator.naturalOrder())
			.collect(Collectors.toList());

		List<String> broadcastStateValues = broadcastResult
			.stream()
			.map(entry -> entry.f1)
			.sorted(Comparator.naturalOrder())
			.collect(Collectors.toList());

		Assert.assertEquals("Unexpected element in broadcast state keys", elements, broadcastStateKeys);

		Assert.assertEquals(
			"Unexpected element in broadcast state values",
			elements.stream().map(Object::toString).sorted().collect(Collectors.toList()),
			broadcastStateValues
		);
	}

	private static class StatefulOperator
		extends BroadcastProcessFunction<Integer, Integer, Void>
		implements CheckpointedFunction {

		private final ListStateDescriptor<Integer> list;
		private final ListStateDescriptor<Integer> union;
		private final MapStateDescriptor<Integer, String> broadcast;

		private List<Integer> elements;

		private ListState<Integer> listState;

		private ListState<Integer> unionState;

		private StatefulOperator(
			ListStateDescriptor<Integer> list,
			ListStateDescriptor<Integer> union,
			MapStateDescriptor<Integer, String> broadcast) {

			this.list = list;
			this.union = union;
			this.broadcast = broadcast;
		}

		@Override
		public void open(Configuration parameters) {
			elements = new ArrayList<>();
		}

		@Override
		public void processElement(Integer value, ReadOnlyContext ctx, Collector<Void> out) {
			elements.add(value);
		}

		@Override
		public void processBroadcastElement(Integer value, Context ctx, Collector<Void> out) throws Exception {
			ctx.getBroadcastState(broadcast).put(value, value.toString());
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			listState.clear();

			listState.addAll(elements);

			unionState.clear();

			unionState.addAll(elements);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			listState = context.getOperatorStateStore().getListState(list);

			unionState = context.getOperatorStateStore().getUnionListState(union);
		}
	}

	interface Runner {
		ListStateDescriptor<Integer> getListStateDescriptor();

		DataSet<Integer> readListState(ExistingSavepoint savepoint) throws IOException;

		ListStateDescriptor<Integer> getUnionStateDescriptor();

		DataSet<Integer> readUnionState(ExistingSavepoint savepoint) throws IOException;

		MapStateDescriptor<Integer, String> getBroadcastStateDescriptor();

		DataSet<Tuple2<Integer, String>> readBroadcastState(ExistingSavepoint savepoint) throws IOException;
	}

	private static class DefaultSerializationRunner implements  Runner {
		private static ListStateDescriptor<Integer> list = new ListStateDescriptor<>(LIST_NAME, Types.INT);

		private static ListStateDescriptor<Integer> union = new ListStateDescriptor<>(UNION_NAME, Types.INT);

		private static MapStateDescriptor<Integer, String> broadcast = new MapStateDescriptor<>(BROADCAST_NAME, Types.INT, Types.STRING);

		@Override
		public ListStateDescriptor<Integer> getListStateDescriptor() {
			return list;
		}

		@Override
		public DataSet<Integer> readListState(ExistingSavepoint savepoint) throws IOException {
			return savepoint.readListState(UID, LIST_NAME, Types.INT);
		}

		@Override
		public DataSet<Integer> readUnionState(ExistingSavepoint savepoint) throws IOException {
			return savepoint.readUnionState(UID, UNION_NAME, Types.INT);
		}

		@Override
		public ListStateDescriptor<Integer> getUnionStateDescriptor() {
			return union;
		}

		@Override
		public DataSet<Tuple2<Integer, String>> readBroadcastState(ExistingSavepoint savepoint) throws IOException {
			return savepoint.readBroadcastState(UID, BROADCAST_NAME, Types.INT, Types.STRING);
		}

		@Override
		public MapStateDescriptor<Integer, String> getBroadcastStateDescriptor() {
			return broadcast;
		}
	}

	private static class CustomSerializationRunner implements Runner {
		private ListStateDescriptor<Integer> list = new ListStateDescriptor<>(
			LIST_NAME, CustomIntSerializer.INSTANCE);

		private ListStateDescriptor<Integer> union = new ListStateDescriptor<>(
			UNION_NAME, CustomIntSerializer.INSTANCE);

		private MapStateDescriptor<Integer, String> broadcast = new MapStateDescriptor<>(
			BROADCAST_NAME, CustomIntSerializer.INSTANCE, StringSerializer.INSTANCE);

		@Override
		public ListStateDescriptor<Integer> getListStateDescriptor() {
			return list;
		}

		@Override
		public DataSet<Integer> readListState(ExistingSavepoint savepoint) throws IOException {
			return savepoint.readListState(UID, LIST_NAME, Types.INT, CustomIntSerializer.INSTANCE);
		}

		@Override
		public ListStateDescriptor<Integer> getUnionStateDescriptor() {
			return union;
		}

		@Override
		public DataSet<Integer> readUnionState(ExistingSavepoint savepoint) throws IOException {
			return savepoint.readUnionState(UID, UNION_NAME, Types.INT, CustomIntSerializer.INSTANCE);
		}

		@Override
		public MapStateDescriptor<Integer, String> getBroadcastStateDescriptor() {
			return broadcast;
		}

		@Override
		public DataSet<Tuple2<Integer, String>> readBroadcastState(ExistingSavepoint savepoint) throws IOException {
			return savepoint.readBroadcastState(UID, BROADCAST_NAME, Types.INT, Types.STRING, CustomIntSerializer.INSTANCE, StringSerializer.INSTANCE);
		}
	}
}
