package org.apache.flink.state.api.output.windowing;

import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.state.api.functions.WindowStateReaderFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class InternalWindowContext<IN, OUT, KEY, W extends Window> extends WindowStateReaderFunction<IN, OUT, KEY, W>.Context {

	public W window;

	public InternalWindowContext(WindowStateReaderFunction<IN, OUT, KEY, W> function) {
		function.super();
	}

	@Override
	public W window() {
		return window;
	}

	@Override
	public long currentProcessingTime() {
		return 0;
	}

	@Override
	public KeyedStateStore windowState() {
		return null;
	}

	@Override
	public KeyedStateStore globalState() {
		return null;
	}
}
