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

package org.apache.flink.connectors.savepoint.output;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.savepoint.runtime.NeverFireProcessingTimeService;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

/**
 * A {@link BoundedOneInputStreamTask} for executing {@link OneInputStreamOperator}'s.
 */
@Internal
class BoundedOneInputStreamTask<IN, OUT> extends BoundedStreamTask<IN, OUT, OneInputStreamOperator<IN, OUT>> {

	@Nullable private final TimestampAssigner<IN> timestampAssigner;

	private final StreamRecord<IN> reuse;

	BoundedOneInputStreamTask(
		Environment environment,
		Path savepointPath,
		Iterable<IN> elements,
		@Nullable TimestampAssigner<IN> timestampAssigner) {
		super(environment, new NeverFireProcessingTimeService(), elements, savepointPath);

		this.timestampAssigner = timestampAssigner;
		this.reuse = new StreamRecord<>(null);
	}

	@Override
	protected void process(IN value) throws Exception {
		reuse.replace(value);

		if (timestampAssigner != null) {
			long timestamp = timestampAssigner.extractTimestamp(value, Long.MIN_VALUE);
			reuse.setTimestamp(timestamp);
		}

		headOperator.setKeyContextElement1(reuse);
		headOperator.processElement(reuse);
	}
}

