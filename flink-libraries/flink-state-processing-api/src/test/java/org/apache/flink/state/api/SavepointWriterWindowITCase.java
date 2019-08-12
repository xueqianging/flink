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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.util.MaxWatermarkSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.StreamCollector;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * IT Test for writing savepoints to the {@code WindowOperator}.
 */
@SuppressWarnings("unchecked")
@RunWith(Parameterized.class)
public class SavepointWriterWindowITCase extends AbstractTestBase {

	private static final String UID = "uid";

	private static final Collection<String> WORDS = Arrays.asList("hello", "world", "hello", "everyone");

	private static final Matcher<Iterable<? extends Tuple2<String, Integer>>> STANDARD_MATCHER = Matchers.containsInAnyOrder(
		Tuple2.of("hello", 2),
		Tuple2.of("world", 1),
		Tuple2.of("everyone", 1)
	);

	private static final Matcher<Iterable<? extends Tuple2<String, Integer>>> EVICTOR_MATCHER = Matchers.containsInAnyOrder(
		Tuple2.of("hello", 1),
		Tuple2.of("world", 1),
		Tuple2.of("everyone", 1)
	);

	private static final TypeInformation<Tuple2<String, Integer>> TUPLE_TYPE_INFO = new TypeHint<Tuple2<String, Integer>>() {}.getTypeInfo();

	private static final List<Tuple3<String, WindowBootstrap, WindowStream>> SETUP_FUNCTIONS = Arrays.asList(
		Tuple3.of(
			"reduce",
			transformation -> transformation.reduce(new Reducer()),
			stream -> stream.reduce(new Reducer())
		),
		Tuple3.of(
			"aggregate",
			transformation -> transformation.aggregate(new Aggregator()),
			stream -> stream.aggregate(new Aggregator())
		),
		Tuple3.of(
			"apply",
			transformation -> transformation.apply(new CustomWindowFunction()),
			stream -> stream.apply(new CustomWindowFunction())
		),
		Tuple3.of(
			"process",
			transformation -> transformation.process(new CustomProcessWindowFunction()),
			stream -> stream.process(new CustomProcessWindowFunction())
		),
		Tuple3.of(
			"min",
			transformation -> transformation.min(0),
			stream -> stream.min(0)
		)
	);

	private static final List<Tuple2<String, StateBackend>> STATE_BACKENDS = Arrays.asList(
		Tuple2.of(
			"MemoryStateBackend",
			new MemoryStateBackend()
		),
		Tuple2.of(
			"RocksDB",
			new RocksDBStateBackend((StateBackend) new MemoryStateBackend())
		)
	);

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {
		List<Object[]> parameterList = new ArrayList<>();
		for (Tuple2<String, StateBackend> stateBackend : STATE_BACKENDS) {
			for (Tuple3<String, WindowBootstrap, WindowStream> setup : SETUP_FUNCTIONS) {
				Object[] parameters = new Object[] {
					stateBackend.f0 + ": " + setup.f0,
					setup.f1,
					setup.f2,
					stateBackend.f1
				};
				parameterList.add(parameters);
			}
		}

		return parameterList;
	}

	@Rule
	public StreamCollector collector = new StreamCollector();

	private final WindowBootstrap windowBootstrap;

	private final WindowStream windowStream;

	private final StateBackend stateBackend;

	@SuppressWarnings("unused")
	public SavepointWriterWindowITCase(String ignore, WindowBootstrap windowBootstrap, WindowStream windowStream, StateBackend stateBackend) {
		this.windowBootstrap = windowBootstrap;
		this.windowStream = windowStream;
		this.stateBackend = stateBackend;
	}

	@Test
	public void testTumbleWindow() throws Exception {
		final String savepointPath = getTempDirPath(new AbstractID().toHexString());

		ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple2<String, Integer>> bootstrapData = bEnv
			.fromCollection(WORDS)
			.map(word -> Tuple2.of(word, 1))
			.returns(TUPLE_TYPE_INFO);

		WindowedOperatorTransformation<Tuple2<String, Integer>, String, TimeWindow> transformation = OperatorTransformation
			.bootstrapWith(bootstrapData)
			.assignTimestamps(record -> 2L)
			.keyBy(tuple -> tuple.f0, Types.STRING)
			.timeWindow(Time.milliseconds(5));

		Savepoint
			.create(stateBackend, 128)
			.withOperator(UID, windowBootstrap.bootstrap(transformation))
			.write(savepointPath);

		bEnv.execute("write state");

		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		sEnv.setStateBackend(stateBackend);

		WindowedStream<Tuple2<String, Integer>, String, TimeWindow> stream = sEnv
			.addSource(new MaxWatermarkSource<Tuple2<String, Integer>>())
			.returns(TUPLE_TYPE_INFO)
			.keyBy(tuple -> tuple.f0)
			.timeWindow(Time.milliseconds(5));

		DataStream<Tuple2<String, Integer>> windowed = windowStream.window(stream).uid(UID);
		CompletableFuture<Collection<Tuple2<String, Integer>>> future = collector.collect(windowed);

		submitJob(savepointPath, sEnv);

		Collection<Tuple2<String, Integer>> results = future.get();
		Assert.assertThat("Incorrect results from bootstrapped windows", results, STANDARD_MATCHER);
	}

	@Test
	public void testTumbleWindowWithEvictor() throws Exception {
		final String savepointPath = getTempDirPath(new AbstractID().toHexString());

		ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple2<String, Integer>> bootstrapData = bEnv
			.fromCollection(WORDS)
			.map(word -> Tuple2.of(word, 1))
			.returns(TUPLE_TYPE_INFO);

		WindowedOperatorTransformation<Tuple2<String, Integer>, String, TimeWindow> transformation = OperatorTransformation
			.bootstrapWith(bootstrapData)
			.assignTimestamps(record -> 2L)
			.keyBy(tuple -> tuple.f0, Types.STRING)
			.timeWindow(Time.milliseconds(5))
			.evictor(CountEvictor.of(1));

		Savepoint
			.create(new MemoryStateBackend(), 128)
			.withOperator(UID, windowBootstrap.bootstrap(transformation))
			.write(savepointPath);

		bEnv.execute("write state");

		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		WindowedStream<Tuple2<String, Integer>, String, TimeWindow> stream = sEnv
			.addSource(new MaxWatermarkSource<>(), TUPLE_TYPE_INFO)
			.keyBy(tuple -> tuple.f0)
			.timeWindow(Time.milliseconds(5))
			.evictor(CountEvictor.of(1));

		DataStream<Tuple2<String, Integer>> windowed = windowStream.window(stream).uid(UID);
		CompletableFuture<Collection<Tuple2<String, Integer>>> future = collector.collect(windowed);

		submitJob(savepointPath, sEnv);

		Collection<Tuple2<String, Integer>> results = future.get();
		Assert.assertThat("Incorrect results from bootstrapped windows", results, EVICTOR_MATCHER);
	}

	@Test
	public void testSlideWindow() throws Exception {
		final String savepointPath = getTempDirPath(new AbstractID().toHexString());

		ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple2<String, Integer>> bootstrapData = bEnv
			.fromCollection(WORDS)
			.map(word -> Tuple2.of(word, 1))
			.returns(TUPLE_TYPE_INFO);

		WindowedOperatorTransformation<Tuple2<String, Integer>, String, TimeWindow> transformation = OperatorTransformation
			.bootstrapWith(bootstrapData)
			.assignTimestamps(record -> 2L)
			.keyBy(tuple -> tuple.f0, Types.STRING)
			.timeWindow(Time.milliseconds(5), Time.milliseconds(1));

		Savepoint
			.create(new MemoryStateBackend(), 128)
			.withOperator(UID, windowBootstrap.bootstrap(transformation))
			.write(savepointPath);

		bEnv.execute("write state");

		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		WindowedStream<Tuple2<String, Integer>, String, TimeWindow> stream = sEnv
			.addSource(new MaxWatermarkSource<Tuple2<String, Integer>>())
			.returns(TUPLE_TYPE_INFO)
			.keyBy(tuple -> tuple.f0)
			.timeWindow(Time.milliseconds(5), Time.milliseconds(1));

		DataStream<Tuple2<String, Integer>> windowed = windowStream.window(stream).uid(UID);
		CompletableFuture<Collection<Tuple2<String, Integer>>> future = collector.collect(windowed);

		submitJob(savepointPath, sEnv);

		Collection<Tuple2<String, Integer>> results = future.get();
		Assert.assertEquals("Incorrect number of results", 15, results.size());
		Assert.assertThat("Incorrect bootstrap state", new HashSet<>(results), STANDARD_MATCHER);
	}

	@Test
	public void testSlideWindowWithEvictor() throws Exception {
		final String savepointPath = getTempDirPath(new AbstractID().toHexString());

		ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple2<String, Integer>> bootstrapData = bEnv
			.fromCollection(WORDS)
			.map(word -> Tuple2.of(word, 1))
			.returns(TUPLE_TYPE_INFO);

		WindowedOperatorTransformation<Tuple2<String, Integer>, String, TimeWindow> transformation = OperatorTransformation
			.bootstrapWith(bootstrapData)
			.assignTimestamps(record -> 2L)
			.keyBy(tuple -> tuple.f0, Types.STRING)
			.timeWindow(Time.milliseconds(5), Time.milliseconds(1))
			.evictor(CountEvictor.of(1));

		Savepoint
			.create(new MemoryStateBackend(), 128)
			.withOperator(UID, windowBootstrap.bootstrap(transformation))
			.write(savepointPath);

		bEnv.execute("write state");

		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		WindowedStream<Tuple2<String, Integer>, String, TimeWindow> stream = sEnv
			.addSource(new MaxWatermarkSource<Tuple2<String, Integer>>())
			.returns(TUPLE_TYPE_INFO)
			.keyBy(tuple -> tuple.f0)
			.timeWindow(Time.milliseconds(5), Time.milliseconds(1))
			.evictor(CountEvictor.of(1));

		DataStream<Tuple2<String, Integer>> windowed = windowStream.window(stream).uid(UID);
		CompletableFuture<Collection<Tuple2<String, Integer>>> future = collector.collect(windowed);

		submitJob(savepointPath, sEnv);

		Collection<Tuple2<String, Integer>> results = future.get();
		Assert.assertEquals("Incorrect number of results", 15, results.size());
		Assert.assertThat("Incorrect bootstrap state", new HashSet<>(results), EVICTOR_MATCHER);
	}

	private void submitJob(String savepointPath, StreamExecutionEnvironment sEnv) throws ProgramInvocationException {
		JobGraph jobGraph = sEnv.getStreamGraph().getJobGraph();
		jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath, true));

		ClusterClient<?> client = miniClusterResource.getClusterClient();
		client.submitJob(jobGraph, SavepointWriterWindowITCase.class.getClassLoader());
	}

	private static class Reducer implements ReduceFunction<Tuple2<String, Integer>> {

		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
			return Tuple2.of(value1.f0, value1.f1 + value2.f1);
		}
	}

	private static class Aggregator implements AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {

		@Override
		public Tuple2<String, Integer> createAccumulator() {
			return null;
		}

		@Override
		public Tuple2<String, Integer> add(Tuple2<String, Integer> value, Tuple2<String, Integer> accumulator) {
			if (accumulator == null) {
				return Tuple2.of(value.f0, value.f1);
			}

			accumulator.f1 += value.f1;
			return accumulator;
		}

		@Override
		public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
			return accumulator;
		}

		@Override
		public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
			a.f1 += b.f1;
			return a;
		}
	}

	private static class CustomWindowFunction implements WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

		@Override
		public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) {
			Iterator<Tuple2<String, Integer>> iterator = input.iterator();
			Tuple2<String, Integer> acc = iterator.next();

			while (iterator.hasNext()) {
				Tuple2<String, Integer> next = iterator.next();
				acc.f1 += next.f1;
			}

			out.collect(acc);
		}
	}

	private static class CustomProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

		@Override
		public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) {
			Iterator<Tuple2<String, Integer>> iterator = elements.iterator();
			Tuple2<String, Integer> acc = iterator.next();

			while (iterator.hasNext()) {
				Tuple2<String, Integer> next = iterator.next();
				acc.f1 += next.f1;
			}

			out.collect(acc);
		}
	}

	@FunctionalInterface
	private interface WindowBootstrap {
		BootstrapTransformation<Tuple2<String, Integer>> bootstrap(WindowedOperatorTransformation<Tuple2<String, Integer>, String, TimeWindow> transformation);
	}

	@FunctionalInterface
	private interface WindowStream {
		SingleOutputStreamOperator<Tuple2<String, Integer>> window(WindowedStream<Tuple2<String, Integer>, String, TimeWindow> stream);
	}
}
