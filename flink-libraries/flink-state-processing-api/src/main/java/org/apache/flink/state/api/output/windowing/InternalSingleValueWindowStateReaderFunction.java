package org.apache.flink.state.api.output.windowing;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.state.api.functions.WindowStateReaderFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Collections;

/**
 * Internal window function for wrapping a {@link ProcessWindowFunction} that takes an {@code Iterable}
 * when the window state is a single value.
 */
public final class InternalSingleValueWindowStateReaderFunction<IN, OUT, KEY, W extends Window>
	extends WrappingFunction<WindowStateReaderFunction<IN, OUT, KEY, W>>
	implements InternalWindowReaderFunction<IN, OUT, KEY, W> {

	private static final long serialVersionUID = 1L;

	private final InternalWindowContext<IN, OUT, KEY, W> ctx;

	public InternalSingleValueWindowStateReaderFunction(WindowStateReaderFunction<IN, OUT, KEY, W> wrappedFunction) {
		super(wrappedFunction);
		ctx = new InternalWindowContext<>(wrappedFunction);
	}

	@Override
	public void readKey(KEY key, final W window, IN input, Collector<OUT> out) throws Exception {
		this.ctx.window = window;

		WindowStateReaderFunction<IN, OUT, KEY, W> wrappedFunction = this.wrappedFunction;
		wrappedFunction.readKey(key, ctx, Collections.singletonList(input), out);
	}

	@Override
	public RuntimeContext getRuntimeContext() {
		throw new RuntimeException("This should never be called.");
	}

	@Override
	public IterationRuntimeContext getIterationRuntimeContext() {
		throw new RuntimeException("This should never be called.");
	}
}
