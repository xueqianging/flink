package org.apache.flink.streaming.api.test;


import org.apache.flink.api.common.typeinfo.TypeInformation;

public class StreamTestEnvironment {

	public static StreamTestEnvironment createEnvironment() {
		return new StreamTestEnvironment();
	}

	private StreamTestEnvironment() {

	}

	public <T> DataStream<T> createStream(TypeInformation<T> typeInfo) {
		return new DataStream<>(typeInfo);
	}
}
