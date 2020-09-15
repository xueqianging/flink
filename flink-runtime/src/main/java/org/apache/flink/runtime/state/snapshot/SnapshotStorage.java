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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;

import java.io.IOException;
import java.io.Serializable;

/**
 * A <b>Snapshot Storage</b> policy defines how the state of a streaming application is
 * checkpointed.
 *
 * <p>For example, {@code JobManagerStorage job manager snapshot storage} and stores checkpoints in the memory of
 * the JobManager. The storage is lightweight and without additional dependencies, but not highly available
 * and supports only small state.
 *
 * <p>The {@code FileSystemStorage file system storage} state checkpoints in a filesystem
 * (typically a replicated highly-available filesystem, like <a href="https://hadoop.apache.org/">HDFS</a>,
 * <a href="https://ceph.com/">Ceph</a>, <a href="https://aws.amazon.com/documentation/s3/">S3</a>,
 * <a href="https://cloud.google.com/storage/">GCS</a>, etc).
 *
 * <h2>Serializability</h2>
 *
 * Snapshot Storage need to be {@link java.io.Serializable serializable}, because they distributed
 * across parallel processes (for distributed execution) together with the streaming application code.
 *
 * <p>Because of that, {@code SnapshotStorage} implementations are meant to be like <i>factories</i>
 * that create the proper stores that provide access to the persistent storage. That way, the
 * Snapshot Storage can be very lightweight (contain only configurations) which makes it easier to
 * be serializable.
 *
 * <h2>Thread Safety</h2>
 *
 * Snapshot storage implementations have to be thread-safe. Multiple threads may be creating
 * storage concurrently.
 */
@PublicEvolving
public interface SnapshotStorage extends Serializable {

	/**
	 * Resolves the given pointer to a checkpoint/savepoint into a checkpoint location. The location
	 * supports reading the checkpoint metadata, or disposing the checkpoint storage location.
	 *
	 * <p>If the state backend cannot understand the format of the pointer (for example because it
	 * was created by a different state backend) this method should throw an {@code IOException}.
	 *
	 * @param externalPointer The external checkpoint pointer to resolve.
	 * @return The checkpoint location handle.
	 *
	 * @throws IOException Thrown, if the state backend does not understand the pointer, or if
	 *                     the pointer could not be resolved due to an I/O error.
	 */
	CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException;

	/**
	 * Creates a storage for checkpoints for the given job. The checkpoint storage is
	 * used to write checkpoint data and metadata.
	 *
	 * @param jobId The job to store checkpoint data for.
	 * @return A checkpoint storage for the given job.
	 *
	 * @throws IOException Thrown if the checkpoint storage cannot be initialized.
	 */
	CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException;

	/**
	 * @param defaultSavepointLocation The default savepoint location.
	 */
	void setDefaultSavepointLocation(Path defaultSavepointLocation);

	/**
	 * Creates a variant of the snapshot storage that applies additional configuration parameters.
	 *
	 * <p>Settings that were directly done on the original state backend object in the application
	 * program typically have precedence over setting picked up from the configuration.
	 *
	 * <p>If no configuration is applied, or if the method directly applies configuration values to
	 * the (mutable) state backend object, this method may return the original state backend object.
	 * Otherwise it typically returns a modified copy.
	 *
	 * @param config The configuration to pick the values from.
	 * @param classLoader The class loader that should be used to load the state backend.
	 * @return A reconfigured snapshot storage.
	 *
	 * @throws IllegalConfigurationException Thrown if the configuration contained invalid entries.
	 */
	SnapshotStorage configure(ReadableConfig config, ClassLoader classLoader) throws IllegalConfigurationException;
}
