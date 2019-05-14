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
import org.apache.flink.connectors.savepoint.functions.StateBootstapFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing {@link
 * StateBootstapFunction}'s.
 */
@Internal
public class StateBootstrapOperator<IN>
	extends AbstractUdfStreamOperator<Void, StateBootstapFunction<IN>>
	implements OneInputStreamOperator<IN, Void> {

	private static final long serialVersionUID = 1L;

	private ContextImpl context;

	public StateBootstrapOperator(StateBootstapFunction<IN> function) {
		super(function);
	}

	@Override
	public void open() throws Exception {
		super.open();
		context = new ContextImpl(getProcessingTimeService());
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		context.element = element;
		userFunction.processElement(element.getValue(), context);
	}

	private class ContextImpl implements StateBootstapFunction.Context {
		private final ProcessingTimeService processingTimeService;

		private StreamRecord<IN> element;

		ContextImpl(ProcessingTimeService processingTimeService) {
			this.processingTimeService = processingTimeService;
		}

		@Override
		public long currentProcessingTime() {
			return processingTimeService.getCurrentProcessingTime();
		}

		@Override
		public Long timestamp() {
			Preconditions.checkState(element != null);

			if (element.hasTimestamp()) {
				return element.getTimestamp();
			} else {
				return null;
			}
		}
	}
}

