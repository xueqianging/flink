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

package org.apache.flink.connectors.savepoint.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.connectors.savepoint.output.OperatorStateReducer;
import org.apache.flink.connectors.savepoint.output.OperatorSubtaskStateReducer;
import org.apache.flink.connectors.savepoint.output.SavepointOutputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.StateBackend;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link Savepoint} is a collection of operator states that can be used to supply initial state
 * when starting a {@link org.apache.flink.streaming.api.datastream.DataStream} job.
 */
public class Savepoint {
	private final StateBackend stateBackend;

	private final int maxParallelism;

	private final Map<String, Operator> operatorsByUid;

	/**
	 * Creates a new savepoint.
	 *
	 * @param stateBackend The state backend of the savepoint used for keyed state.
	 * @param maxParallelism The max parallelism of the savepoint.
	 * @return A new savepoint.
	 */
	public static Savepoint create(StateBackend stateBackend, int maxParallelism) {
		return new Savepoint(stateBackend, maxParallelism);
	}

	private Savepoint(StateBackend stateBackend, int maxParallelism) {
		this.stateBackend = stateBackend;
		this.maxParallelism = maxParallelism;

		this.operatorsByUid = new HashMap<>();
	}

	/**
	 * Adds a new operator to the savepoint.
	 *
	 * @param uid The uid of an operator.
	 * @param operator An operator.
	 * @return The savepoint.
	 */
	public Savepoint withOperator(String uid, Operator operator) {
		if (operatorsByUid.containsKey(uid)) {
			throw new IllegalArgumentException("Duplicate uid " + uid + ". All uid's must be unique");
		}

		operatorsByUid.put(uid, operator);
		return this;
	}

	/**
	 * Write the {@link Savepoint} to the file system and location defined by the given URI.
	 *
	 * <p>A file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or 's3://')
	 * must be accessible via {@link org.apache.flink.core.fs.FileSystem#get(URI)})}.
	 *
	 * <p>For a state backend targeting HDFS, this means that the URI must either specify the
	 * authority (host and port), or that the Hadoop configuration that describes that information
	 * must be in the classpath.
	 *
	 * @param savepointPath The URI describing the filesystem (scheme and optionally authority), and
	 *     the path to the checkpoint data directory.
	 */
	public void write(String savepointPath) {
		write(new Path(savepointPath));
	}

	/**
	 * Write the {@link Savepoint} to the file system and location defined by the given URI.
	 *
	 * <p>A file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or 's3://')
	 * must be accessible via {@link org.apache.flink.core.fs.FileSystem#get(URI)})}.
	 *
	 * <p>For a state backend targeting HDFS, this means that the URI must either specify the
	 * authority (host and port), or that the Hadoop configuration that describes that information
	 * must be in the classpath.
	 *
	 * @param savepointPath The URI describing the filesystem (scheme and optionally authority), and
	 *     the path to the checkpoint data directory.
	 */
	public void write(Path savepointPath) {
		operatorsByUid
			.entrySet()
			.stream()
			.map(entry -> getOperatorStates(entry.getKey(), entry.getValue(), savepointPath))
			.reduce(DataSet::union)
			.orElseThrow(() -> new IllegalStateException("Savepoints must contain at least one operator"))
			.reduceGroup(new OperatorStateReducer())
			.output(new SavepointOutputFormat(savepointPath));
	}

	private DataSet<OperatorState> getOperatorStates(String uid, Operator operator, Path savepointPath) {
		return operator
			.getOperatorSubtaskStates(uid, stateBackend, maxParallelism, savepointPath)
			.reduceGroup(new OperatorSubtaskStateReducer(uid, maxParallelism));
	}

}
