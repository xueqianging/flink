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

package org.apache.flink.runtime.state.snapshot;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.memory.MemoryBackendCheckpointStorage;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * This snapshot storage checkpoints state directly to the JobManager's memory (hence the name),
 * but the checkpoints will be persisted to a file system for high-availability setups and savepoints.
 * The JobManagerStorage is consequently a FileSystem-based backend that can work without a
 * file system dependency in simple setups.
 *
 * <p>This storage policy should be used only for experimentation, quick local setups,
 * or for streaming applications that have very small state: Because it requires checkpoints to
 * go through the JobManager's memory, larger state will occupy larger portions of the JobManager's
 * main memory, reducing operational stability.
 * For any other setup, {@link FileSystemStorage}
 * should be used. {@link FileSystemStorage} writes
 * checkpoints state directly to files rather than to the JobManager's memory, thus supporting
 * large state sizes.
 *
 * <h1>State Size Considerations</h1>
 *
 * <p>State checkpointing with this state backend is subject to the following conditions:
 * <ul>
 *     <li>Each individual state must not exceed the configured maximum state size
 *         (see {@link #getMaxStateSize()}.</li>
 *
 *     <li>All state from one task (i.e., the sum of all operator states and keyed states from all
 *         chained operators of the task) must not exceed what the RPC system supports, which is
 *         be default < 10 MB. That limit can be configured up, but that is typically not advised.</li>
 *
 *     <li>The sum of all states in the application times all retained checkpoints must comfortably
 *         fit into the JobManager's JVM heap space.</li>
 * </ul>
 *
 * <h1>Persistence Guarantees</h1>
 *
 * <p>For the use cases where the state sizes can be handled by this backend, the backend does guarantee
 * persistence for savepoints, externalized checkpoints (of configured), and checkpoints
 * (when high-availability is configured).
 *
 * <h1>Configuration</h1>
 *
 * <p>As for all state backends, this backend can either be configured within the application (by creating
 * the backend with the respective constructor parameters and setting it on the execution environment)
 * or by specifying it in the Flink configuration.
 *
 * <p>If the state backend was specified in the application, it may pick up additional configuration
 * parameters from the Flink configuration. For example, if the backend if configured in the application
 * without a default savepoint directory, it will pick up a default savepoint directory specified in the
 * Flink configuration of the running job/cluster. That behavior is implemented via the
 * {@link #configure(ReadableConfig, ClassLoader)} method.
 */
@PublicEvolving
public class JobManagerStorage extends AbstractFsSnapshotStorage {

	/** The default maximal size that the snapshotted memory state may have (5 MiBytes). */
	public static final MemorySize DEFAULT_MAX_STATE_SIZE = MemorySize.ofMebiBytes(5);

	/** The maximal size that the snapshotted memory state may have. */
	private final MemorySize maxStateSize;

	/**
	 * Creates a new memory state backend that accepts states whose serialized forms are
	 * up to the default state size (5 MB).
	 *
	 * <p>Checkpoint and default savepoint locations are used as specified in the
	 * runtime configuration.
	 */
	public JobManagerStorage() {
		this((String) null, DEFAULT_MAX_STATE_SIZE);
	}

	/**
	 * Creates a new memory state backend that accepts states whose serialized forms are
	 * up to the provided value.
	 *
	 * @param maxStateSize The maximal size of the serialized state.
	 *
	 * <p>Default savepoint locations are used as specified in the
	 * runtime configuration.
	 */
	public JobManagerStorage(MemorySize maxStateSize) {
		this((String) null, maxStateSize);
	}

	/**
	 * Creates a new memory state backend that accepts states whose serialized forms are
	 * up to the default state size (5 MB).
	 *
	 * @param checkpointsBaseDirectory The path to write checkpoint metadata to. If null, the value from
	 *                       the runtime configuration will be used.
	 *
	 * <p>Default savepoint locations are used as specified in the
	 * runtime configuration.
	 */
	public JobManagerStorage(Path checkpointsBaseDirectory) {
		this(checkpointsBaseDirectory, DEFAULT_MAX_STATE_SIZE);
	}

	/**
	 * Creates a new memory state backend that accepts states whose serialized forms are
	 * up to the default state size (5 MB).
	 *
	 * @param checkpointsBaseDirectory The path to write checkpoint metadata to. If null, the value from
	 *                       the runtime configuration will be used.
	 *
	 * <p>Default savepoint locations are used as specified in the
	 * runtime configuration.
	 */
	public JobManagerStorage(String checkpointsBaseDirectory) {
		this(checkpointsBaseDirectory, DEFAULT_MAX_STATE_SIZE);
	}

	/**
	 * Private constructor that creates a re-configured copy of the state backend.
	 *
	 * @param original The state backend to re-configure
	 * @param configuration The configuration
	 */
	private JobManagerStorage(JobManagerStorage original, ReadableConfig configuration) {
		super(original.getCheckpointPath(), original.getSavepointPath(), configuration);

		this.maxStateSize = original.maxStateSize;
	}

	/**
	 * Creates a new memory state backend that accepts states whose serialized forms are
	 * up to the provided value.
	 *
	 * @param checkpointsBaseDirectory The path to write checkpoint metadata to. If null, the value from
	 *                       the runtime configuration will be used.
	 * @param maxStateSize The maximal size of the serialized state.
	 *
	 * <p>Default savepoint locations are used as specified in the
	 * runtime configuration.
	 */
	public JobManagerStorage(@Nullable String checkpointsBaseDirectory, MemorySize maxStateSize) {
		super(checkpointsBaseDirectory);
		this.maxStateSize = Preconditions.checkNotNull(maxStateSize, "max state size cannot be null");
	}

	/**
	 * Creates a new memory state backend that accepts states whose serialized forms are
	 * up to the provided value.
	 *
	 * @param checkpointsBaseDirectory The path to write checkpoint metadata to. If null, the value from
	 *                       the runtime configuration will be used.
	 * @param maxStateSize The maximal size of the serialized state.
	 *
	 * <p>Default savepoint locations are used as specified in the
	 * runtime configuration.
	 */
	public JobManagerStorage(@Nullable Path checkpointsBaseDirectory, MemorySize maxStateSize) {
		super(checkpointsBaseDirectory);
		this.maxStateSize = Preconditions.checkNotNull(maxStateSize, "max state size cannot be null");
	}

	@Override
	public CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException {
		int size = MathUtils.checkedDownCast(maxStateSize.getBytes());
		return new MemoryBackendCheckpointStorage(jobId, getCheckpointPath(), getSavepointPath(), size);
	}

	@Override
	public JobManagerStorage configure(ReadableConfig config, ClassLoader classLoader) throws IllegalConfigurationException {
		return new JobManagerStorage(this, config);
	}

	public MemorySize getMaxStateSize() {
		return maxStateSize;
	}

	@Override
	public String toString() {
		return "JobManagerStorage{" +
			"maxStateSize=" + maxStateSize +
			", checkpointDirectory" + getCheckpointPath() +
			", savepointDirectory" + getSavepointPath() +
			'}';
	}
}
