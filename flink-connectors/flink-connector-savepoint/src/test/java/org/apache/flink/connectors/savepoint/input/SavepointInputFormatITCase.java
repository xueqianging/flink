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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * IT case for reading operator state.
 */
public class SavepointInputFormatITCase extends AbstractTestBase {
	private static final String uid = "stateful-operator";

	private static ListStateDescriptor<Integer> list = new ListStateDescriptor<>("list", Types.INT);

	private static ListStateDescriptor<Integer> union = new ListStateDescriptor<>("union", Types.INT);

	private static MapStateDescriptor<Integer, String> broadcast = new MapStateDescriptor<>("broadcast", Types.INT, Types.STRING);

	@Test
	public void testOperatorStateInputFormat() throws Exception {
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.setParallelism(4);

		DataStream<Integer> data = streamEnv
			.addSource(new SavepointSource())
			.rebalance();

		data
			.connect(data.broadcast(broadcast))
			.process(new StatefulOperator())
			.uid(uid)
			.addSink(new DiscardingSink<>());

		JobGraph jobGraph = streamEnv.getStreamGraph().getJobGraph();

		String savepoint = takeSavepoint(jobGraph);

		ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();

		verifyListState(savepoint, batchEnv);

		verifyUnionState(savepoint, batchEnv);

		verifyBroadcastState(savepoint, batchEnv);
	}

	private void verifyListState(String savepoint, ExecutionEnvironment batchEnv) throws Exception {
		ListStateInputFormat<Integer> listInput = new ListStateInputFormat<>(savepoint, SavepointInputFormatITCase.uid, list);

		List<Integer> listResult = batchEnv.createInput(listInput, Types.INT).collect();
		listResult.sort(Comparator.naturalOrder());

		Assert.assertEquals("Unexpected elements read from list state", SavepointSource.getElements(), listResult);
	}

	private void verifyUnionState(String savepoint, ExecutionEnvironment batchEnv) throws Exception {
		UnionStateInputFormat<Integer> unionInput = new UnionStateInputFormat<>(savepoint, SavepointInputFormatITCase.uid, union);

		List<Integer> unionResult = batchEnv.createInput(unionInput, Types.INT).collect();
		unionResult.sort(Comparator.naturalOrder());

		Assert.assertEquals("Unexpected elements read from union state", SavepointSource.getElements(), unionResult);
	}

	private void verifyBroadcastState(String savepoint, ExecutionEnvironment batchEnv) throws Exception {
		BroadcastStateInputFormat<Integer, String> broadcastFormat = new BroadcastStateInputFormat<>(savepoint, SavepointInputFormatITCase.uid, broadcast);
		List<Map.Entry<Integer, String>> broadcastResult = batchEnv
			.createInput(broadcastFormat, new TypeHint<Map.Entry<Integer, String>>(){}.getTypeInfo())
			.collect();

		List<Integer> broadcastStateKeys  = broadcastResult.
			stream()
			.map(Map.Entry::getKey)
			.sorted(Comparator.naturalOrder())
			.collect(Collectors.toList());

		List<String> broadcastStateValues = broadcastResult
			.stream()
			.map(Map.Entry::getValue)
			.sorted(Comparator.naturalOrder())
			.collect(Collectors.toList());

		Assert.assertEquals("Unexpected element in broadcast state keys", SavepointSource.getElements(), broadcastStateKeys);

		Assert.assertEquals(
			"Unexpected element in broadcast state values",
			SavepointSource.getElements().stream().map(Object::toString).sorted().collect(Collectors.toList()),
			broadcastStateValues
		);
	}

	private String takeSavepoint(JobGraph jobGraph) throws Exception {
		SavepointSource.initializeForTest();

		ClusterClient<?> client = miniClusterResource.getClusterClient();
		client.setDetached(true);

		JobID jobId = jobGraph.getJobID();

		Deadline deadline = Deadline.fromNow(Duration.ofMinutes(5));

		String dirPath = getTempDirPath(new AbstractID().toHexString());

		try {
			client.setDetached(true);
			JobSubmissionResult result = client.submitJob(jobGraph, SavepointInputFormatITCase.class.getClassLoader());

			boolean finished = false;
			while (deadline.hasTimeLeft()) {
				if (SavepointSource.isFinished()) {
					finished = true;

					break;
				}
			}

			if (!finished) {
				Assert.fail("Failed to initialize state within deadline");
			}

			CompletableFuture<String> path = client.triggerSavepoint(result.getJobID(), dirPath);
			return path.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
		} finally {
			client.cancel(jobId);
		}
	}

	private static class SavepointSource implements SourceFunction<Integer> {
		private static volatile boolean finished;

		private volatile boolean running = true;

		private static final Integer[] elements = {1, 2, 3};

		@Override
		public void run(SourceContext<Integer> ctx) {
			synchronized (ctx.getCheckpointLock()) {
				for (Integer element : elements) {
					ctx.collect(element);
				}

				finished = true;
			}

			while (running) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// ignore
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		private static void initializeForTest() {
			finished = false;
		}

		private static boolean isFinished() {
			return finished;
		}

		private static List<Integer> getElements() {
			return Arrays.asList(elements);
		}
	}

	private static class StatefulOperator
		extends BroadcastProcessFunction<Integer, Integer, Void>
		implements CheckpointedFunction {

		private List<Integer> elements;

		private ListState<Integer> listState;

		private ListState<Integer> unionState;

		@Override
		public void open(Configuration parameters) throws Exception {
			elements = new ArrayList<>();
		}

		@Override
		public void processElement(Integer value, ReadOnlyContext ctx, Collector<Void> out) throws Exception {
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
}
