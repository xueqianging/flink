/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.savepoint.input;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.savepoint.runtime.OperatorIdUtil;
import org.apache.flink.connectors.savepoint.runtime.SavepointLoader;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.io.IOException;
import java.util.function.Function;

/**
 * Base input format for consuming state from {@link Savepoint}'s.
 */
abstract class SavepointInputFormat<O, T extends InputSplit> extends RichInputFormat<O, T> {
	private final String savepointPath;

	private final String uid;

	private final OperatorID operatorID;

	private transient SavepointStatistics statistics;

	/**
	 * @param savepointPath A pointer to a {@link Savepoint}.
	 * @param uid A valid uid in the savepoint.
	 */
	SavepointInputFormat(String savepointPath, String uid) {
		this.savepointPath = savepointPath;
		this.uid = uid;

		this.operatorID = OperatorIdUtil.operatorIDFromUID(uid);
	}

	@Override
	public final InputSplitAssigner getInputSplitAssigner(T[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return statistics;
	}

	/**
	 * Create the {@link BaseStatistics} for the specified operator.
	 *
	 * @param operatorState The operator that the input format will read.
	 * @param subtaskStateSize A function from {@link OperatorSubtaskState} to size in bytes.
	 */
	final void createStatistics(OperatorState operatorState, Function<OperatorSubtaskState, Long> subtaskStateSize) {
		final long size = operatorState
			.getStates()
			.stream()
			.map(subtaskStateSize)
			.reduce(0L, Long::sum);

		statistics = new SavepointStatistics(size);
	}

	@Override
	public final void configure(Configuration parameters) {

	}

	/**
	 * Finds the {@link OperatorState} for a uid within a savepoint.
	 * @return A handle to the operator state in the savepoint with the provided uid.
	 * @throws IOException If the savepoint path is invalid or the uid does not exist
	 */
	OperatorState getOperatorState() throws IOException {
		final Savepoint savepoint = SavepointLoader.loadSavepoint(savepointPath, this.getClass().getClassLoader());

		for (final OperatorState state : savepoint.getOperatorStates()) {
			if (state.getOperatorID().equals(operatorID)) {
				return state;
			}
		}

		throw new IOException("Savepoint does not contain state with operator uid " + uid);
	}

	final String getUid() {
		return uid;
	}

	final OperatorID getOperatorID() {
		return operatorID;
	}

	private static class SavepointStatistics implements BaseStatistics {
		private final long inputSize;

		SavepointStatistics(long inputSize) {
			this.inputSize = inputSize;
		}

		@Override
		public long getTotalInputSize() {
			return inputSize;
		}

		@Override
		public long getNumberOfRecords() {
			return BaseStatistics.NUM_RECORDS_UNKNOWN;
		}

		@Override
		public float getAverageRecordWidth() {
			return BaseStatistics.AVG_RECORD_BYTES_UNKNOWN;
		}
	}
}
