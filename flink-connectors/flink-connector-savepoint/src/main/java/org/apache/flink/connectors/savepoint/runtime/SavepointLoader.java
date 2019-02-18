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

package org.apache.flink.connectors.savepoint.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage;
import org.apache.flink.runtime.state.memory.MemoryBackendCheckpointStorage;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Utility for loading savepoint metadata.
 */
@Internal
public final class SavepointLoader {
	private SavepointLoader() {}

	private static final JobID jobId = new JobID(0, 0);

	/**
	 * Takes the given string (representing a pointer to a checkpoint) and resolves it to a file
	 * status for the checkpoint's metadata file.
	 *
	 * @param savepointPath The path to an external savepoint.
	 * @param userClassLoader  The ClassLoader for user code classes.
	 * @return A state handle to savepoint's metadata.
	 *
	 * @throws IOException Thrown, if the path cannot be resolved, the file system not accessed, or
	 *                     the path points to a location that does not seem to be a savepoint.
	 */
	public static Savepoint loadSavepoint(String savepointPath, ClassLoader userClassLoader) throws IOException {
		AbstractFsCheckpointStorage storage = new MemoryBackendCheckpointStorage(jobId, null, null, Integer.MAX_VALUE);
		CompletedCheckpointStorageLocation location = storage.resolveCheckpoint(savepointPath);

		return Checkpoints.loadCheckpointMetadata(
			new DataInputStream(location.getMetadataHandle().openInputStream()),
			userClassLoader);
	}
}
