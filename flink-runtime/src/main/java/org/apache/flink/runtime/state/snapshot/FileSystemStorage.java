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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.filesystem.AbstractFileStateBackend;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorage;
import org.apache.flink.util.MathUtils;

import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.net.URI;

import static org.apache.flink.configuration.CheckpointingOptions.FS_SMALL_FILE_THRESHOLD;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This snapshot storage policy checkpoints state as files to a file system (hence the name).
 *
 * <p>Each checkpoint individually will store all its files in a subdirectory that includes the
 * checkpoint number, such as {@code hdfs://namenode:port/flink-checkpoints/chk-17/}.
 *
 * <h1>Persistence Guarantees</h1>
 *
 * <p>Checkpoints from this state backend are as persistent and available as filesystem that is written to.
 * If the file system is a persistent distributed file system, this state backend supports
 * highly available setups. The backend additionally supports savepoints and externalized checkpoints.
 *
 * <h1>Configuration</h1>
 *
 * <p>As for all snapshot, this backend can either be configured within the application (by creating
 * the backend with the respective constructor parameters and setting it on the checkpoint configuration)
 * or by specifying it in the Flink configuration.
 *
 * <p>If the snapshot storage was specified in the application, it may pick up additional configuration
 * parameters from the Flink configuration. For example, if the storage if configured in the application
 * without a default savepoint directory, it will pick up a default savepoint directory specified in the
 * Flink configuration of the running job/cluster. That behavior is implemented via the
 * {@link #configure(ReadableConfig, ClassLoader)} method.
 */
@PublicEvolving
public class FileSystemStorage extends AbstractFsSnapshotStorage {

	private static final long serialVersionUID = 1L;

	/** Maximum size of state that is stored with the metadata, rather than in files (1 MiByte). */
	private static final MemorySize MAX_FILE_STATE_THRESHOLD = MemorySize.ofMebiBytes(1);

	// ------------------------------------------------------------------------

	/**
	 * State below this size will be stored as part of the metadata, rather than in files.
	 * A value of 'null' means not yet configured, in which case the default will be used.
	 */
	private final MemorySize fileStateThreshold;

	/**
	 * The write buffer size for created checkpoint stream, this should not be less than file state threshold when we want
	 * state below that threshold stored as part of metadata not files.
	 * A value of 'null' means not yet configured, in which case the default will be used.
	 * */
	private final MemorySize writeBufferSize;

	// -----------------------------------------------------------------------

	/**
	 * Creates a new snapshot storage that stores its checkpoint data in the file system and location
	 * defined by the given URI.
	 *
	 * <p>A file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or 'S3://')
	 * must be accessible via {@link FileSystem#get(URI)}.
	 *
	 * <p>For a snapshot storage targeting HDFS, this means that the URI must either specify the authority
	 * (host and port), or that the Hadoop configuration that describes that information must be in the
	 * classpath.
	 *
	 * @param checkpointDataUri The URI describing the filesystem (scheme and optionally authority),
	 *                          and the path to the checkpoint data directory.
	 */
	public FileSystemStorage(String checkpointDataUri) {
		this(new Path(checkpointDataUri));
	}


	/**
	 * Creates a new snapshot storage that stores its checkpoint data in the file system and location
	 * defined by the given URI.
	 *
	 * <p>A file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or 'S3://')
	 * must be accessible via {@link FileSystem#get(URI)}.
	 *
	 * <p>For a snapshot storage targeting HDFS, this means that the URI must either specify the authority
	 * (host and port), or that the Hadoop configuration that describes that information must be in the
	 * classpath.
	 *
	 * @param checkpointDataUri The URI describing the filesystem (scheme and optionally authority),
	 *                          and the path to the checkpoint data directory.
	 */
	public FileSystemStorage(Path checkpointDataUri) {
		this(checkpointDataUri.toUri());
	}

	/**
	 * Creates a new snapshot storage that stores its checkpoint data in the file system and location
	 * defined by the given URI.
	 *
	 * <p>A file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or 'S3://')
	 * must be accessible via {@link FileSystem#get(URI)}.
	 *
	 * <p>For a snapshot storage targeting HDFS, this means that the URI must either specify the authority
	 * (host and port), or that the Hadoop configuration that describes that information must be in the
	 * classpath.
	 *
	 * @param checkpointDataUri The URI describing the filesystem (scheme and optionally authority),
	 *                          and the path to the checkpoint data directory.
	 */
	public FileSystemStorage(URI checkpointDataUri) {
		this(checkpointDataUri, null, null);
	}

	/**
	 * Creates a new snapshot storage that stores its checkpoint data in the file system and location
	 * defined by the given URI.
	 *
	 * <p>A file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or 'S3://')
	 * must be accessible via {@link FileSystem#get(URI)}.
	 *
	 * <p>For a snapshot storage targeting HDFS, this means that the URI must either specify the authority
	 * (host and port), or that the Hadoop configuration that describes that information must be in the
	 * classpath.
	 *
	 * @param checkpointDataUri The URI describing the filesystem (scheme and optionally authority),
	 *                          and the path to the checkpoint data directory.
	 * @param fileStateSizeThreshold State up to this size will be stored as part of the metadata,
	 *                             rather than in files
	 */
	public FileSystemStorage(URI checkpointDataUri, MemorySize fileStateSizeThreshold) {
		this(checkpointDataUri, fileStateSizeThreshold, null);
	}

	/**
	 * Creates a new snapshot storage that stores its checkpoint data in the file system and location
	 * defined by the given URI.
	 *
	 * <p>A file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or 'S3://')
	 * must be accessible via {@link FileSystem#get(URI)}.
	 *
	 * <p>For a snapshot storage targeting HDFS, this means that the URI must either specify the authority
	 * (host and port), or that the Hadoop configuration that describes that information must be in the
	 * classpath.
	 *
	 * @param checkpointDirectory        The path to write checkpoint metadata to.
	 *
	 * @param fileStateSizeThreshold     State below this size will be stored as part of the metadata,
	 *                                   rather than in files. If null, the value configured in the
	 *                                   runtime configuration will be used, or the default value (1KB)
	 *                                   if nothing is configured.
	 * @param writeBufferSize            Write buffer size used to serialize state. If null, the value configured in the
	 *                                   runtime configuration will be used, or the default value (4KB)
	 *                                   if nothing is configured.
	 */
	public FileSystemStorage(
		URI checkpointDirectory,
		MemorySize fileStateSizeThreshold,
		MemorySize writeBufferSize) {

		super(checkNotNull(checkpointDirectory, "checkpoint directory is null"));

		checkArgument(fileStateSizeThreshold == null || fileStateSizeThreshold.getBytes() <= MAX_FILE_STATE_THRESHOLD.getBytes(),
			"The threshold for file state size must be in [0, %s], or 'null' which means to use " +
				"the value from the deployment's configuration.", MAX_FILE_STATE_THRESHOLD);

		this.fileStateThreshold = fileStateSizeThreshold;
		this.writeBufferSize = writeBufferSize;
	}

	/**
	 * Private constructor that creates a re-configured copy of the snapshot storage.
	 *
	 * @param original The snapshot storage to re-configure
	 * @param configuration The configuration
	 */
	private FileSystemStorage(FileSystemStorage original, ReadableConfig configuration) {
		super(original.getCheckpointPath(), original.getSavepointPath(), configuration);

		if (getValidFileStateThreshold(original.fileStateThreshold) != null) {
			this.fileStateThreshold = original.fileStateThreshold;
		} else {
			final MemorySize configuredStateThreshold =
				getValidFileStateThreshold(configuration.get(FS_SMALL_FILE_THRESHOLD));

			if (configuredStateThreshold != null) {
				this.fileStateThreshold = configuredStateThreshold;
			} else {
				this.fileStateThreshold = FS_SMALL_FILE_THRESHOLD.defaultValue();

				// because this is the only place we (unlikely) ever log, we lazily
				// create the logger here
				LoggerFactory.getLogger(AbstractFileStateBackend.class).warn(
					"Ignoring invalid file size threshold value ({}): {} - using default value {} instead.",
					FS_SMALL_FILE_THRESHOLD.key(), configuration.get(FS_SMALL_FILE_THRESHOLD).getBytes(),
					FS_SMALL_FILE_THRESHOLD.defaultValue());
			}
		}

		final MemorySize bufferSize = original.writeBufferSize != null ?
			original.writeBufferSize :
			new MemorySize(configuration.get(CheckpointingOptions.FS_WRITE_BUFFER_SIZE));

		this.writeBufferSize = new MemorySize(Math.max(bufferSize.getBytes(), this.fileStateThreshold.getBytes()));
	}

	private MemorySize getValidFileStateThreshold(MemorySize fileStateThreshold) {
		if (fileStateThreshold != null && fileStateThreshold.getBytes() <= MAX_FILE_STATE_THRESHOLD.getBytes()) {
			return fileStateThreshold;
		}
		return null;
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the base directory where all the checkpoints are stored.
	 * The job-specific checkpoint directory is created inside this directory.
	 *
	 * @return The base directory for checkpoints.
	 */
	@Nonnull
	@Override
	public Path getCheckpointPath() {
		// we know that this can never be null by the way of constructor checks
		//noinspection ConstantConditions
		return super.getCheckpointPath();
	}

	/**
	 * Gets the threshold below which state is stored as part of the metadata, rather than in files.
	 * This threshold ensures that the backend does not create a large amount of very small files,
	 * where potentially the file pointers are larger than the state itself.
	 *
	 * <p>If not explicitly configured, this is the default value of
	 * {@link CheckpointingOptions#FS_SMALL_FILE_THRESHOLD}.
	 *
	 * @return The file size threshold, in bytes.
	 */
	public int getMinFileSizeThreshold() {
		return fileStateThreshold != null ?
			MathUtils.checkedDownCast(fileStateThreshold.getBytes()) :
			MathUtils.checkedDownCast(FS_SMALL_FILE_THRESHOLD.defaultValue().getBytes());
	}

	/**
	 * Gets the write buffer size for created checkpoint stream.
	 *
	 * <p>If not explicitly configured, this is the default value of
	 * {@link CheckpointingOptions#FS_WRITE_BUFFER_SIZE}.
	 *
	 * @return The write buffer size, in bytes.
	 */
	public int getWriteBufferSize() {
		return writeBufferSize != null ?
			MathUtils.checkedDownCast(writeBufferSize.getBytes()) :
			CheckpointingOptions.FS_WRITE_BUFFER_SIZE.defaultValue();
	}

	// ------------------------------------------------------------------------
	//  Reconfiguration
	// ------------------------------------------------------------------------

	/**
	 * Creates a copy of this snapshot storage that uses the values defined in the configuration
	 * for fields where that were not specified in this snapshot storage.
	 *
	 * @param config the configuration
	 * @return The re-configured variant of the snapshot storage
	 */
	@Override
	public FileSystemStorage configure(ReadableConfig config, ClassLoader classLoader) {
		return new FileSystemStorage(this, config);
	}

	// ------------------------------------------------------------------------
	//  initialization and cleanup
	// ------------------------------------------------------------------------

	@Override
	public CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException {
		checkNotNull(jobId, "jobId");
		return new FsCheckpointStorage(
			getCheckpointPath(),
			getSavepointPath(),
			jobId,
			getMinFileSizeThreshold(),
			getWriteBufferSize());
	}

	@Override
	public String toString() {
		return "FileSystemStorage{" +
			"fileStateThreshold=" + fileStateThreshold +
			", writeBufferSize=" + writeBufferSize +
			", checkpointDirectory" + getCheckpointPath() +
			", savepointDirectory" + getSavepointPath() +
			'}';
	}
}
