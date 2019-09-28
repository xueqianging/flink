package org.apache.flink.state.api.output.windowing;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Internal interface for functions that are evaluated over keyed (grouped) windows.
 *
 * @param <IN> The type of the input value.
 * @param <OUT> The type of the output value.
 * @param <KEY> The type of the key.
 */
public interface InternalWindowReaderFunction<IN, OUT, KEY, W extends Window> extends Function {
	/**
	 * Evaluates the window and outputs none or several elements.
	 *
	 * @param input The elements in the window being evaluated.
	 * @param out A collector for emitting elements.
	 *
	 * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
	 */
	void readKey(KEY key, W window, IN input, Collector<OUT> out) throws Exception;
}
