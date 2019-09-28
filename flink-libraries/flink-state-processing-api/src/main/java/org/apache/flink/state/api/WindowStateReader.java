package org.apache.flink.state.api;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.input.WindowStateInputFormat;
import org.apache.flink.state.api.output.windowing.InternalSingleValueWindowStateReaderFunction;
import org.apache.flink.state.api.output.windowing.PassThroughWindowReaderFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;

@SuppressWarnings("WeakerAccess")
public class WindowStateReader<W extends Window> {

	private static final String WINDOW_STATE_NAME = "window-contents";

	private final ExecutionEnvironment env;

	private final OperatorState operatorState;

	private final StateBackend stateBackend;

	private final TypeSerializer<W> namespaceSerializer;

	public WindowStateReader(ExecutionEnvironment env, OperatorState operatorState, StateBackend stateBackend, TypeSerializer<W> namespaceSerializer) {
		this.env = env;
		this.operatorState = operatorState;
		this.stateBackend = stateBackend;
		this.namespaceSerializer = namespaceSerializer;
	}

	public <K, OUT> DataSet<OUT> reduce(ReduceFunction<OUT> function, TypeInformation<K> keyType, TypeInformation<OUT> typeInfo) {
		ReducingStateDescriptor<OUT> descriptor = new ReducingStateDescriptor<OUT>(WINDOW_STATE_NAME, function, typeInfo);

		PassThroughWindowReaderFunction<K, OUT, W> windowReaderFunction = new PassThroughWindowReaderFunction<>();

		WindowStateInputFormat<OUT, OUT, OUT, K, W> inputFormat = new WindowStateInputFormat<>(
			operatorState,
			stateBackend,
			keyType,
			new InternalSingleValueWindowStateReaderFunction<>(windowReaderFunction),
			namespaceSerializer,
			descriptor
		);

		return env.createInput(inputFormat, typeInfo);
	}
}
