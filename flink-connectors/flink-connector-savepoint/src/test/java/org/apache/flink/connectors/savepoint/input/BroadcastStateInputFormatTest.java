package org.apache.flink.connectors.savepoint.input;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connectors.savepoint.input.splits.OperatorStateInputSplit;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithNonKeyedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link BroadcastStateInputFormat}.
 */
public class BroadcastStateInputFormatTest {
	private static MapStateDescriptor<Integer, Integer> descriptor = new MapStateDescriptor<>("state", Types.INT, Types.INT);

	@Test
	public void testReadBroadcastState() throws Exception {
		try (TwoInputStreamOperatorTestHarness<Void, Integer, Void> testHarness = new TwoInputStreamOperatorTestHarness<>(
			new CoBroadcastWithNonKeyedOperator<>(new StatefulFunction(), Collections.singletonList(descriptor))
		)) {
			testHarness.open();

			testHarness.processElement2(new StreamRecord<>(1));
			testHarness.processElement2(new StreamRecord<>(2));
			testHarness.processElement2(new StreamRecord<>(3));

			OperatorSubtaskState state = testHarness.snapshot(0, 0);

			OperatorStateInputSplit split = new OperatorStateInputSplit(state.getManagedOperatorState(), 0);

			BroadcastStateInputFormat<Integer, Integer> format = new BroadcastStateInputFormat<>("n/a", "uid", descriptor);

			format.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));
			format.open(split);

			Map<Integer, Integer> results = new HashMap<>(3);

			while (!format.reachedEnd()) {
				Map.Entry<Integer, Integer> entry = format.nextRecord(null);
				results.put(entry.getKey(), entry.getValue());
			}

			Map<Integer, Integer> expected = new HashMap<>(3);
			expected.put(1, 1);
			expected.put(2, 2);
			expected.put(3, 3);

			Assert.assertEquals(
				"Failed to read correct list state from state backend",
				expected,
				results);
		}
	}

	static class StatefulFunction extends BroadcastProcessFunction<Void, Integer, Void> {

		@Override
		public void processElement(Void value, ReadOnlyContext ctx, Collector<Void> out) throws Exception {

		}

		@Override
		public void processBroadcastElement(Integer value, Context ctx, Collector<Void> out) throws Exception {
			ctx.getBroadcastState(descriptor).put(value, value);
		}
	}
}
