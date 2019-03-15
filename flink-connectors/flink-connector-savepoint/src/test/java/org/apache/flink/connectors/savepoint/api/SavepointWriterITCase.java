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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.savepoint.functions.KeyedProcessWriterFunction;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
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
	public void testStateBootstrap() throws Exception {
		final String uid = "accounts";
		final String savepointPath = getTempDirPath(new AbstractID().toHexString());

		ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Account> data = bEnv.fromCollection(accounts);

		Operator operator = Operator
			.fromDataSet(data)
			.assignTimestamps((account, _unused) -> account.timestamp)
			.keyBy(acc -> acc.id)
			.process(new AccountBootstrapper());

		Savepoint
			.create(backend, 128)
			.withOperator(uid, operator)
			.write(savepointPath);

		bEnv.execute("Bootstrap");

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
	public static class AccountBootstrapper extends KeyedProcessWriterFunction<Integer, Account> {
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

