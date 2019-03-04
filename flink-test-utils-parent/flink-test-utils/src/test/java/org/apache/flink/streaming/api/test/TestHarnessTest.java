package org.apache.flink.streaming.api.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestHarnessTest {

	@Test
	public void testOneInputHarness() throws Exception {
		StreamTestEnvironment env = StreamTestEnvironment.createEnvironment();

		OneInputTestHarness<Integer, Integer> harness = env
			.createStream(Types.INT)
			.map((MapFunction<Integer, Integer>) value -> value);

		List<Integer> results = harness.processElement(10);

		Assert.assertEquals("Failed to push element through harness", 1, results.size());
		Assert.assertEquals("Failed to push element through harness", 10, results.get(0).intValue());
	}

	@Test
	public void testEventTime() throws Exception {
		StreamTestEnvironment env = StreamTestEnvironment.createEnvironment();

		OneInputTestHarness<Integer, Integer> harness = env
			.createStream(Types.INT)
			.keyBy(id -> id)
			.process(new KeyedProcessFunction<Integer, Integer, Integer>() {
				ValueState<Integer> state;

				@Override
				public void open(Configuration parameters) throws Exception {
					state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("state", Types.INT));
				}

				@Override
				public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
					state.update(value);

					ctx.timerService().registerEventTimeTimer(100);
				}

				@Override
				public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
					Assert.assertEquals("Incorrect timestamp for timer", 100, timestamp);
					Assert.assertEquals("Wrong time semantic", TimeDomain.EVENT_TIME, ctx.timeDomain());

					out.collect(state.value());
				}
			});

		harness.processElement(10);

		List<Integer> results = harness.setEventTime(100);

		Assert.assertEquals("Failed to push element through harness", 1, results.size());
		Assert.assertEquals("Failed to push element through harness", 10, results.get(0).intValue());
	}

	@Test
	public void testProcessingTime() throws Exception {
		StreamTestEnvironment env = StreamTestEnvironment.createEnvironment();

		OneInputTestHarness<Integer, Integer> harness = env
			.createStream(Types.INT)
			.keyBy(id -> id)
			.process(new KeyedProcessFunction<Integer, Integer, Integer>() {
				ValueState<Integer> state;

				@Override
				public void open(Configuration parameters) throws Exception {
					state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("state", Types.INT));
				}

				@Override
				public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
					state.update(value);

					ctx.timerService().registerProcessingTimeTimer(100);
				}

				@Override
				public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
					Assert.assertEquals("Incorrect timestamp for timer", 100, timestamp);
					Assert.assertEquals("Wrong time semantic", TimeDomain.PROCESSING_TIME, ctx.timeDomain());

					out.collect(state.value());
				}
			});

		harness.processElement(10);

		List<Integer> results = harness.setProcessingTime(100);

		Assert.assertEquals("Failed to push element through harness", 1, results.size());
		Assert.assertEquals("Failed to push element through harness", 10, results.get(0).intValue());
	}
}
