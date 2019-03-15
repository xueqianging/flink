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

package org.apache.flink.connectors.savepoint.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.savepoint.functions.KeyedProcessWriterFunction;
import org.apache.flink.connectors.savepoint.runtime.VoidTriggerable;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing {@link
 * KeyedProcessWriterFunction}'s.
 */
@Internal
public class KeyedProcessWriterOperator<K, IN>
	extends AbstractUdfStreamOperator<Void, KeyedProcessWriterFunction<K, IN>>
	implements OneInputStreamOperator<IN, Void> {

	private static final long serialVersionUID = 1L;

	private transient KeyedProcessWriterOperator<K, IN>.ContextImpl context;

	public KeyedProcessWriterOperator(KeyedProcessWriterFunction<K, IN> function) {
		super(function);
	}

	@Override
	public void open() throws Exception {
		super.open();

		InternalTimerService<VoidNamespace> internalTimerService = getInternalTimerService(
			"user-timers",
			VoidNamespaceSerializer.INSTANCE,
			VoidTriggerable.instance());

		TimerService timerService = new SimpleTimerService(internalTimerService);

		context = new KeyedProcessWriterOperator<K, IN>.ContextImpl(userFunction, timerService);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		context.element = element;
		userFunction.processElement(element.getValue(), context);
		context.element = null;
	}

	private class ContextImpl extends KeyedProcessWriterFunction<K, IN>.Context {

		private final TimerService timerService;

		private StreamRecord<IN> element;

		ContextImpl(KeyedProcessWriterFunction<K, IN> function, TimerService timerService) {
			function.super();
			this.timerService = checkNotNull(timerService);
		}

		@Override
		public Long timestamp() {
			checkState(element != null);

			if (element.hasTimestamp()) {
				return element.getTimestamp();
			} else {
				return null;
			}
		}

		@Override
		public TimerService timerService() {
			return timerService;
		}

		@Override
		@SuppressWarnings("unchecked")
		public K getCurrentKey() {
			return (K) KeyedProcessWriterOperator.this.getCurrentKey();
		}
	}
}

