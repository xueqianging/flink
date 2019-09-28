/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.api.input;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.internal.InternalAppendingState;
import org.apache.flink.state.api.output.windowing.InternalWindowReaderFunction;
import org.apache.flink.state.api.runtime.SavepointRuntimeContext;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * An input format for reading state from a {@code WindowOperator}.
 *
 * @param <IN> The input type to the window.
 * @param <ACC> The accumulator type to the window.
 * @param <OUT> The output type of the accumulator.
 * @param <KEY>
 * @param <W>
 */
@Internal
public class WindowStateInputFormat<IN, ACC, OUT, KEY, W extends Window>
	extends KeyedStateBaseInputFormat<Tuple2<KEY, W>, KEY, W, OUT, InternalWindowReaderFunction<ACC, OUT, KEY, W>> {

	private static final String WINDOW_TIMER_NAME = "window-timers";

	private transient InternalAppendingState<KEY, W, IN, ACC, ACC> windowState;

	private final StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor;

	public WindowStateInputFormat(
		OperatorState operatorState,
		StateBackend stateBackend,
		TypeInformation<KEY> keyType,
		InternalWindowReaderFunction<ACC, OUT, KEY, W> userFunction,
		TypeSerializer<W> namespaceSerializer,
		StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor) {
		super(operatorState, stateBackend, keyType, WINDOW_TIMER_NAME, userFunction, namespaceSerializer);
		this.windowStateDescriptor = windowStateDescriptor;
	}

	@Override
	Iterator<Tuple2<KEY, W>> open(SavepointRuntimeContext ctx) throws Exception {
		this.windowState = (InternalAppendingState<KEY, W, IN, ACC, ACC>) keyedStateBackend.getOrCreateKeyedState(namespaceSerializer, windowStateDescriptor);
		Stream<Tuple2<KEY, W>> data = keyedStateBackend.getKeysAndNamespaces(windowStateDescriptor.getName());
		return new IteratorWithRemove<>(data.iterator());
	}

	@Override
	void readKey(Tuple2<KEY, W> key, Collector<OUT> out) throws Exception {
		windowState.setCurrentNamespace(key.f1);
		ACC contents = windowState.get();
		userFunction.readKey(key.f0, key.f1, contents, out);
	}

	@Override
	KEY getUserKey(Tuple2<KEY, W> record) {
		return record.f0;
	}

	private static class IteratorWithRemove<T> implements Iterator<T> {

		private final Iterator<T> iterator;

		private IteratorWithRemove(Iterator<T> iterator) {
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public T next() {
			return iterator.next();
		}

		@Override
		public void remove() { }
	}
}
