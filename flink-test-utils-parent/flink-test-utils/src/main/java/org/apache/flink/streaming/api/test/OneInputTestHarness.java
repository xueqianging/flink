package org.apache.flink.streaming.api.test;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.stream.Collectors;

public class OneInputTestHarness<T, V> implements AutoCloseable {

	private final OneInputStreamOperatorTestHarness<T, V> harness;

	private long currentWatermark;

	private long currentProcessingTime;

	OneInputTestHarness(OneInputStreamOperatorTestHarness<T, V> harness) throws Exception {
		this.harness = harness;

		this.currentWatermark = Long.MIN_VALUE;
		this.currentProcessingTime = Long.MIN_VALUE;

		harness.setup();
		harness.open();
	}

	@Override
	public void close() throws Exception {
		harness.close();
	}

	public List<V> processElement(T element, long timestamp) throws Exception {
		harness.processElement(element, timestamp);

		return getOutputAndReset();
	}

	public List<V> processElement(T element) throws Exception {
		return processElement(element, Long.MIN_VALUE);
	}

	public List<V> setEventTime(long timestamp) throws Exception {
		Preconditions.checkState(timestamp >= currentWatermark, "Event time must be monotonically increasing");

		currentWatermark = timestamp;
		harness.processWatermark(timestamp);

		return getOutputAndReset();
	}

	public List<V> setProcessingTime(long timestamp) throws Exception {
		Preconditions.checkState(timestamp >= currentProcessingTime, "Processing time must be monotonically increasing");

		currentProcessingTime = timestamp;
		harness.setProcessingTime(timestamp);

		return getOutputAndReset();
	}

	private List<V> getOutputAndReset() {
		List<V> output = harness
			.extractOutputStreamRecords()
			.stream()
			.map(StreamRecord::getValue)
			.collect(Collectors.toList());

		harness.getOutput().clear();

		return output;
	}
}
