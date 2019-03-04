package org.apache.flink.streaming.api.test;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

public class DataStream<T> {
	private final TypeInformation<T> type;

	DataStream(TypeInformation<T> type) {
		this.type = type;
	}

	public TypeInformation<T> getType() {
		return type;
	}

	public <K> KeyedStream<K, T> keyBy(KeySelector<T, K> selector) {
		TypeInformation<K> typeInfo = TypeExtractor.getKeySelectorTypes(selector, getType());
		return new KeyedStream<>(selector, typeInfo, getType());
	}

	public <K> KeyedStream<K, T> keyBy(KeySelector<T, K> selector, TypeInformation<K> typeInfo) {
		return new KeyedStream<>(selector, typeInfo, getType());
	}

	public <R> OneInputTestHarness<T, R> map(MapFunction<T, R> function) throws Exception {
		return createTestHarness(new StreamMap<>(function));
	}
	
	public <R> OneInputTestHarness<T, R> flatMap(FlatMapFunction<T, R> function) throws Exception {
		return createTestHarness(new StreamFlatMap<>(function));
	}
	
	public <R> OneInputTestHarness<T, R> process(ProcessFunction<T, R> function) throws Exception {
		return createTestHarness(new ProcessOperator<>(function));
	}

	public OneInputTestHarness<T, T> filter(FilterFunction<T> function) throws Exception {
		return createTestHarness(new StreamFilter<>(function));
	}

	public <R> OneInputTestHarness<T, R> transform(OneInputStreamOperator<T, R> operator) throws Exception {
		return createTestHarness(operator);
	}

	protected <R> OneInputTestHarness<T, R> createTestHarness(OneInputStreamOperator<T, R> operator) throws Exception {
		OneInputStreamOperatorTestHarness<T, R> harness = new OneInputStreamOperatorTestHarness<>(operator);
		return new OneInputTestHarness<>(harness);
	}
}
