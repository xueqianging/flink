package org.apache.flink.state.api.output.windowing;

import org.apache.flink.state.api.functions.WindowStateReaderFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class PassThroughWindowReaderFunction<KEY, IN, W extends Window> extends WindowStateReaderFunction<IN, IN, KEY, W> {

	private static final long serialVersionUID = 1L;

	@Override
	public void readKey(KEY key, Context context, Iterable<IN> elements, Collector<IN> out) throws Exception {
		for (IN element : elements) {
			out.collect(element);
		}
	}
}
