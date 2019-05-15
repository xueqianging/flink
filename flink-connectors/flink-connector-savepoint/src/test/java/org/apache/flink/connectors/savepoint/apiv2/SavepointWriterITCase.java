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

package org.apache.flink.connectors.savepoint.apiv2;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.savepoint.functions.KeyedStateBootstrapFunction;
import org.apache.flink.connectors.savepoint.functions.StateBootstapFunction;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * IT test for writing savepoints.
 */
@RunWith(value = Parameterized.class)
public class SavepointWriterITCase extends AbstractTestBase {
	private final StateBackend backend;

	private static final Collection<Account> accounts = Arrays.asList(
		new Account(1, 100.0),
		new Account(2, 100.0),
		new Account(3, 100.0));

	public SavepointWriterITCase(StateBackend backend) {
		this.backend = backend;
	}

	@Parameterized.Parameters(name = "Savepoint Writer: {0}")
	public static Collection<StateBackend> data() {
		return Arrays.asList(
			new MemoryStateBackend(),
			new RocksDBStateBackend((StateBackend) new MemoryStateBackend()));
	}

	@Test
	public void testStateBootstrapAndModification() throws Exception {
		final String uid = "accounts";
		final String modify = "numbers";

		final String savepointPath = getTempDirPath(new AbstractID().toHexString());

		bootstrapState(uid, savepointPath);

		validateBootstrap(uid, savepointPath);

		final String modifyPath = getTempDirPath(new AbstractID().toHexString());

		modifySavepoint(modify, savepointPath, modifyPath);

		validateModification(uid, modify, modifyPath);
	}

	private void bootstrapState(String uid, String savepointPath) throws Exception {
		ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Account> data = bEnv.fromCollection(accounts);

		BootstrapTransformation<Account> transformation = OperatorTransformation
			.bootstrapWith(data)
			.assignTimestamps(account -> account.timestamp)
			.keyBy(acc -> acc.id)
			.transform(new AccountBootstrapper());

		Savepoint
			.create(backend, 128)
			.withOperator(uid, transformation)
			.write(savepointPath);

		bEnv.execute("Bootstrap");
	}

	private void validateBootstrap(String uid, String savepointPath) throws ProgramInvocationException {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		sEnv.setStateBackend(backend);

		CollectSink.accountList.clear();

		sEnv.fromCollection(accounts)
			.keyBy(acc -> acc.id)
			.flatMap(new UpdateAndGetAccount())
			.uid(uid)
			.addSink(new CollectSink());

		JobGraph jobGraph = sEnv.getStreamGraph().getJobGraph();
		jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath, true));

		ClusterClient<?> client = miniClusterResource.getClusterClient();
		client.submitJob(jobGraph, SavepointWriterITCase.class.getClassLoader());

		Assert.assertEquals("Unexpected output", 3, CollectSink.accountList.size());
	}

	private void modifySavepoint(String modify, String savepointPath, String modifyPath) throws Exception {
		ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Integer> data = bEnv.fromElements(1, 2, 3);

		BootstrapTransformation<Integer> transformation = OperatorTransformation
			.bootstrapWith(data)
			.transform(new ModifyProcessFunction());

		Savepoint
			.load(bEnv, savepointPath, backend)
			.withOperator(modify, transformation)
			.write(modifyPath);

		bEnv.execute("Modifying");
	}

	private void validateModification(String uid, String modify, String savepointPath) throws ProgramInvocationException {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		sEnv.setStateBackend(backend);

		CollectSink.accountList.clear();

		DataStream<Account> stream = sEnv.fromCollection(accounts)
			.keyBy(acc -> acc.id)
			.flatMap(new UpdateAndGetAccount())
			.uid(uid);

		stream.addSink(new CollectSink());

		stream
			.map(acc -> acc.id)
			.map(new StatefulOperator())
			.uid(modify)
			.addSink(new DiscardingSink<>());

		JobGraph jobGraph = sEnv.getStreamGraph().getJobGraph();
		jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath, true));

		ClusterClient<?> client = miniClusterResource.getClusterClient();
		client.submitJob(jobGraph, SavepointWriterITCase.class.getClassLoader());

		Assert.assertEquals("Unexpected output", 3, CollectSink.accountList.size());
	}

	/**
	 * A simple pojo.
	 */
	public static class Account {
		Account(int id, double amount) {
			this.id = id;
			this.amount = amount;
			this.timestamp = 1000L;
		}

		public int id;

		public double amount;

		public long timestamp;

		@Override
		public boolean equals(Object obj) {
			return obj instanceof Account && ((Account) obj).id == id && ((Account) obj).amount == amount;
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, amount);
		}
	}

	/**
	 * A savepoint writer function.
	 */
	public static class AccountBootstrapper extends KeyedStateBootstrapFunction<Integer, Account> {
		ValueState<Double> state;

		@Override
		public void open(Configuration parameters) throws Exception {
			ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>("total", Types.DOUBLE);
			state = getRuntimeContext().getState(descriptor);
		}

		@Override
		public void processElement(Account value, Context ctx) throws Exception {
			state.update(value.amount);
		}
	}

	/**
	 * A streaming function bootstrapped off the state.
	 */
	public static class UpdateAndGetAccount extends RichFlatMapFunction<Account, Account> {
		ValueState<Double> state;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>("total", Types.DOUBLE);
			state = getRuntimeContext().getState(descriptor);
		}

		@Override
		public void flatMap(Account value, Collector<Account> out) throws Exception {
			Double current = state.value();
			if (current != null) {
				value.amount += current;
			}

			state.update(value.amount);
			out.collect(value);
		}
	}

	/**
	 * A bootstrap function.
	 */
	public static class ModifyProcessFunction extends StateBootstapFunction<Integer> {
		List<Integer> numbers;

		ListState<Integer> state;

		@Override
		public void open(Configuration parameters) throws Exception {
			numbers = new ArrayList<>();
		}

		@Override
		public void processElement(Integer value, Context ctx) throws Exception {
			numbers.add(value);
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			state.clear();
			state.addAll(numbers);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			state = context.getOperatorStateStore().getUnionListState(
				new ListStateDescriptor<>("numbers", Types.INT)
			);
		}
	}

	/**
	 * A streaming function bootstrapped off the state.
	 */
	public static class StatefulOperator extends RichMapFunction<Integer, Integer> implements CheckpointedFunction {
		List<Integer> numbers;

		ListState<Integer> state;

		@Override
		public void open(Configuration parameters) throws Exception {
			numbers = new ArrayList<>();
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			state.clear();
			state.addAll(numbers);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			state = context.getOperatorStateStore().getUnionListState(
				new ListStateDescriptor<>("numbers", Types.INT)
			);

			if (context.isRestored()) {
				Set<Integer> expected = new HashSet<>();
				expected.add(1);
				expected.add(2);
				expected.add(3);

				for (Integer number : state.get()) {
					Assert.assertTrue("Duplicate state", expected.contains(number));
					expected.remove(number);
				}

				Assert.assertTrue("Failed to bootstrap all state elements: " + Arrays.toString(expected.toArray()), expected.isEmpty());
			}
		}

		@Override
		public Integer map(Integer value) throws Exception {
			return null;
		}
	}

	/**
	 * A simple collections sink.
	 */
	public static class CollectSink implements SinkFunction<Account> {
		static Set<Integer> accountList = new ConcurrentSkipListSet<>();

		@Override
		public void invoke(Account value, Context context) {
			accountList.add(value.id);
		}
	}
}
