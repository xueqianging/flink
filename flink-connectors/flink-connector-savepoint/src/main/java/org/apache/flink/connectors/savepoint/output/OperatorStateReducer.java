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
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV2;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A reducer that aggregates multiple {@link OperatorState}'s into a single {@link Savepoint}.
 */
@Internal
public class OperatorStateReducer implements GroupReduceFunction<OperatorState, Savepoint> {
	private final Collection<MasterState> masterStates;

	public OperatorStateReducer() {
		masterStates = Collections.emptyList();
	}

	public OperatorStateReducer(Collection<MasterState> masterStates) {
		this.masterStates = masterStates;
	}

	@Override
	public void reduce(Iterable<OperatorState> values, Collector<Savepoint> out) {
		Savepoint savepoint =
			new SavepointV2(
				0,
				StreamSupport.stream(values.spliterator(), false).collect(Collectors.toList()),
				masterStates);

		out.collect(savepoint);
	}
}

