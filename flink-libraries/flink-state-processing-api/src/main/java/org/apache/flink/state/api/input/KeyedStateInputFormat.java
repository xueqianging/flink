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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.api.runtime.SavepointRuntimeContext;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Input format for reading partitioned state.
 *
 * @param <K> The type of the key.
 * @param <OUT> The type of the output of the {@link KeyedStateReaderFunction}.
 */
@Internal
public class KeyedStateInputFormat<K, OUT> extends KeyedStateBaseInputFormat<K, K, VoidNamespace, OUT, KeyedStateReaderFunction<K, OUT>> {

	private static final long serialVersionUID = 8230460226049597182L;

	private static final String USER_TIMERS_NAME = "user-timers";

	private transient Context ctx;

	/**
	 * Creates an input format for reading partitioned state from an operator in a savepoint.
	 *
	 * @param operatorState The state to be queried.
	 * @param stateBackend  The state backed used to snapshot the operator.
	 * @param keyType       The type information describing the key type.
	 * @param userFunction  The {@link KeyedStateReaderFunction} called for each key in the operator.
	 */
	public KeyedStateInputFormat(
		OperatorState operatorState,
		StateBackend stateBackend,
		TypeInformation<K> keyType,
		KeyedStateReaderFunction<K, OUT> userFunction) {
		super(operatorState, stateBackend, keyType, USER_TIMERS_NAME, userFunction, VoidNamespaceSerializer.INSTANCE);
	}

	@Override
	Iterator<K> open(SavepointRuntimeContext ctx) throws IOException{
		try {
			this.ctx = new Context<>(keyedStateBackend, timerService);
		} catch (Exception e) {
			throw new IOException("Failed to restore timer state", e);
		}

		return new MultiStateKeyIterator<>(ctx.getStateDescriptors(), keyedStateBackend);
	}

	@Override
	void readKey(K key, Collector<OUT> out) throws Exception {
		userFunction.readKey(key, ctx, out);
	}

	@Override
	K getUserKey(K record) {
		return record;
	}

	private static class Context<K> implements KeyedStateReaderFunction.Context {

		private static final String EVENT_TIMER_STATE = "event-time-timers";

		private static final String PROC_TIMER_STATE = "proc-time-timers";

		ListState<Long> eventTimers;

		ListState<Long> procTimers;

		private Context(AbstractKeyedStateBackend<K> keyedStateBackend, InternalTimerService<VoidNamespace> timerService) throws Exception {
			eventTimers = keyedStateBackend.getPartitionedState(
				USER_TIMERS_NAME,
				StringSerializer.INSTANCE,
				new ListStateDescriptor<>(EVENT_TIMER_STATE, Types.LONG)
			);

			timerService.forEachEventTimeTimer((namespace, timer) -> {
				if (namespace.equals(VoidNamespace.INSTANCE)) {
					eventTimers.add(timer);
				}
			});

			procTimers = keyedStateBackend.getPartitionedState(
				USER_TIMERS_NAME,
				StringSerializer.INSTANCE,
				new ListStateDescriptor<>(PROC_TIMER_STATE, Types.LONG)
			);

			timerService.forEachProcessingTimeTimer((namespace, timer) -> {
				if (namespace.equals(VoidNamespace.INSTANCE)) {
					procTimers.add(timer);
				}
			});
		}

		@Override
		public Set<Long> registeredEventTimeTimers() throws Exception {
			Iterable<Long> timers = eventTimers.get();
			if (timers == null) {
				return Collections.emptySet();
			}

			return StreamSupport
				.stream(timers.spliterator(), false)
				.collect(Collectors.toSet());
		}

		@Override
		public Set<Long> registeredProcessingTimeTimers() throws Exception {
			Iterable<Long> timers = procTimers.get();
			if (timers == null) {
				return Collections.emptySet();
			}

			return StreamSupport
				.stream(timers.spliterator(), false)
				.collect(Collectors.toSet());
		}
	}
}
