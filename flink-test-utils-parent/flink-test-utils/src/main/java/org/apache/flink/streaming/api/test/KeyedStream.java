package org.apache.flink.streaming.api.test;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamGroupedReduce;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

public class KeyedStream<K, T> extends DataStream<T> {

	private final KeySelector<T, K> selector;

	private final TypeInformation<K> typeInfo;

	public KeyedStream(KeySelector<T, K> selector, TypeInformation<K> typeInfo, TypeInformation<T> type) {
		super(type);

		this.selector = selector;
		this.typeInfo = typeInfo;
	}
	
	public <R> OneInputTestHarness<T, R> process(KeyedProcessFunction<K, T, R> function) throws Exception{
		return createTestHarness(new KeyedProcessOperator<>(function));
	}

	public OneInputTestHarness<T, T> reduce(ReduceFunction<T> function) throws Exception {
		return createTestHarness(new StreamGroupedReduce<>(function, getType().createSerializer(new ExecutionConfig())));
	}

	@Override
	protected <R> OneInputTestHarness<T, R> createTestHarness(OneInputStreamOperator<T, R> operator) throws Exception {
		KeyedOneInputStreamOperatorTestHarness<K, T, R> harness = new KeyedOneInputStreamOperatorTestHarness<>(operator, selector, typeInfo);
		return new OneInputTestHarness<>(harness);
	} 
}
