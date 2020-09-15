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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.snapshot.factory.FileSystemStorageFactory;
import org.apache.flink.runtime.state.snapshot.factory.JobManagerStorageFactory;
import org.apache.flink.runtime.state.snapshot.factory.SnapshotStorageFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.DynamicCodeLoadingException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

/**
 * Tests to validate snapshot storage instances
 * are properly loaded based on user configurations.
 */
public class SnapshotStorageLoadingTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private final ClassLoader cl = getClass().getClassLoader();

	@Test
	public void testNoSnapshotStorageDefined() throws Exception {
		assertNull(SnapshotStorageLoader.loadSnapshotStorageFromConfig(new Configuration(), cl, null));
	}

	@Test
	public void testLegacyBackendAlwaysWins() throws Exception {
		SnapshotStorage storage = SnapshotStorageLoader
			.fromApplicationOrConfigOrDefault(
				null,
				null,
				new LegacyStateBackend(),
				new Configuration(),
				cl,
				null);

		assertThat("legacy state backends should always take precedence", storage, instanceOf(LegacyStateBackend.class));
	}

	@Test
	public void testInstantiateJobManagerSnapshotStorageByDefault() throws Exception {
		SnapshotStorage storage = SnapshotStorageLoader
			.fromApplicationOrConfigOrDefault(
				null,
				null,
				new ModernStateBackend(),
				new Configuration(),
				cl,
				null);

		assertThat("wrong snapshot storage loaded", storage, instanceOf(JobManagerStorage.class));
	}

	@Test
	public void testInstantiateFsSnapshotStorageByDefaultWhenCheckpointDirectoryProvided() throws Exception {
		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		final Configuration config1 = new Configuration();
		config1.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);

		SnapshotStorage storage = SnapshotStorageLoader
			.fromApplicationOrConfigOrDefault(
				null,
				null,
				new ModernStateBackend(),
				config1,
				cl,
				null);

		assertThat("wrong snapshot storage loaded", storage, instanceOf(FileSystemStorage.class));
	}

	@Test
	public void testConfigSavepointDir() throws Exception {
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();
		final Path expectedSavepointPath = new Path(savepointDir);
		final Configuration config = new Configuration();
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

		SnapshotStorage storage = SnapshotStorageLoader
			.fromApplicationOrConfigOrDefault(
				new SimpleSnapshotStorage(),
				null,
				new ModernStateBackend(),
				config,
				cl,
				null);

		assertThat("wrong snapshot storage loaded", storage, instanceOf(SimpleSnapshotStorage.class));

		SimpleSnapshotStorage simple = (SimpleSnapshotStorage) storage;
		assertEquals("wrong savepoint directory set", expectedSavepointPath, simple.defaultSavepointLocation);
	}

	@Test
	public void testAppSavepointDirAlwaysWins() throws Exception {
		final String savepointDirConfig = new Path(tmp.newFolder().toURI()).toString();
		final Path savepointDirApp = new Path(tmp.newFolder().toURI());
		final Configuration config = new Configuration();
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDirConfig);

		SnapshotStorage storage = SnapshotStorageLoader
			.fromApplicationOrConfigOrDefault(
				new SimpleSnapshotStorage(),
				savepointDirApp,
				new ModernStateBackend(),
				config,
				cl,
				null);

		assertThat("wrong snapshot storage loaded", storage, instanceOf(SimpleSnapshotStorage.class));

		SimpleSnapshotStorage simple = (SimpleSnapshotStorage) storage;
		assertEquals("wrong savepoint directory set", savepointDirApp, simple.defaultSavepointLocation);
	}

	// ------------------------------------------------------------------------
	//  Failures
	// ------------------------------------------------------------------------

	/**
	 * This test makes sure that failures properly manifest when the state backend could not be loaded.
	 */
	@Test
	public void testLoadingFails() throws Exception {
		final Configuration config = new Configuration();

		// try a value that is neither recognized as a name, nor corresponds to a class
		config.setString(CheckpointingOptions.SNAPSHOT_STORAGE, "does.not.exist");
		try {
			SnapshotStorageLoader
				.fromApplicationOrConfigOrDefault(
					null,
					null,
					new ModernStateBackend(),
					config,
					cl,
					null);

			fail("should fail with an exception");
		} catch (DynamicCodeLoadingException ignored) {
			// expected
		}

		// try a class that is not a factory
		config.setString(CheckpointingOptions.SNAPSHOT_STORAGE, java.io.File.class.getName());
		try {
			SnapshotStorageLoader
				.fromApplicationOrConfigOrDefault(
					null,
					null,
					new ModernStateBackend(),
					config,
					cl,
					null);

			fail("should fail with an exception");
		} catch (DynamicCodeLoadingException ignored) {
			// expected
		}

		// a factory that fails
		config.setString(CheckpointingOptions.SNAPSHOT_STORAGE, FailingFactory.class.getName());
		try {
			SnapshotStorage storage = SnapshotStorageLoader
				.fromApplicationOrConfigOrDefault(
					null,
					null,
					new ModernStateBackend(),
					config,
					cl,
					null);

			fail("should fail with an exception");
		} catch (IOException ignored) {
			// expected
		}
	}

	// ------------------------------------------------------------------------
	//  JobManager Storage
	// ------------------------------------------------------------------------

	/**
	 * Validates loading a job manager snapshot storage from the cluster configuration.
	 */
	@Test
	public void testLoadJobManagerStorageNoParameters() throws Exception {
		// we configure with the explicit string
		// to guard against config-breaking changes of the name

		final Configuration config1 = new Configuration();
		config1.setString(CheckpointingOptions.SNAPSHOT_STORAGE, "jobmanager");

		final Configuration config2 = new Configuration();
		config2.setString(CheckpointingOptions.SNAPSHOT_STORAGE, JobManagerStorageFactory.class.getName());

		SnapshotStorage storage1 = SnapshotStorageLoader.loadSnapshotStorageFromConfig(config1, cl, null);
		SnapshotStorage storage2 = SnapshotStorageLoader.loadSnapshotStorageFromConfig(config2, cl, null);

		assertThat("wrong snapshot storage loaded", storage1, instanceOf(JobManagerStorage.class));
		assertThat("wrong snapshot storage loaded", storage2, instanceOf(JobManagerStorage.class));
	}

	/**
	 * Validates loading a job manager snapshot storage with additional parameters from the cluster configuration.
	 */
	@Test
	public void testLoadJobManagerStorageWithParameters() throws Exception {
		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();
		final Path expectedCheckpointPath = new Path(checkpointDir);
		final Path expectedSavepointPath = new Path(savepointDir);

		// we configure with the explicit string
		// to guard against config-breaking changes of the name

		final Configuration config1 = new Configuration();
		config1.setString(CheckpointingOptions.SNAPSHOT_STORAGE, "jobmanager");
		config1.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		config1.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

		final Configuration config2 = new Configuration();
		config2.setString(CheckpointingOptions.SNAPSHOT_STORAGE, JobManagerStorageFactory.class.getName());
		config2.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		config2.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

		JobManagerStorage storage1 = (JobManagerStorage)
			SnapshotStorageLoader.loadSnapshotStorageFromConfig(config1, cl, null);
		JobManagerStorage storage2 = (JobManagerStorage)
			SnapshotStorageLoader.loadSnapshotStorageFromConfig(config2, cl, null);

		assertNotNull(storage1);
		assertNotNull(storage2);

		assertEquals(expectedCheckpointPath, storage1.getCheckpointPath());
		assertEquals(expectedCheckpointPath, storage2.getCheckpointPath());
		assertEquals(expectedSavepointPath, storage1.getSavepointPath());
		assertEquals(expectedSavepointPath, storage2.getSavepointPath());
	}

	/**
	 * Validates taking the application-defined job manager snapshot storage and adding additional
	 * parameters from the cluster configuration.
	 */
	@Test
	public void testConfigureJobManagerStorage() throws Exception {
		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();
		final Path expectedCheckpointPath = new Path(checkpointDir);
		final Path expectedSavepointPath = new Path(savepointDir);

		final MemorySize maxSize = MemorySize.ofMebiBytes(1);

		final JobManagerStorage storage = new JobManagerStorage(maxSize);

		final Configuration config = new Configuration();
		config.setString(CheckpointingOptions.SNAPSHOT_STORAGE, "filesystem"); // check that this is not accidentally picked up
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

		SnapshotStorage loadedStorage = SnapshotStorageLoader
			.fromApplicationOrConfigOrDefault(
				storage,
				null,
				new ModernStateBackend(),
				config,
				cl,
				null);

		assertThat(loadedStorage, instanceOf(JobManagerStorage.class));

		final JobManagerStorage jmStorage = (JobManagerStorage) loadedStorage;
		assertEquals(expectedCheckpointPath, jmStorage.getCheckpointPath());
		assertEquals(expectedSavepointPath, jmStorage.getSavepointPath());
		assertEquals(maxSize, jmStorage.getMaxStateSize());
	}

	/**
	 * Validates taking the application-defined memory state backend and adding additional
	 * parameters from the cluster configuration, but giving precedence to application-defined
	 * parameters over configuration-defined parameters.
	 */
	@Test
	public void testConfigureJobManagerStorageMixed() throws Exception {
		final String appCheckpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();

		final Path expectedCheckpointPath = new Path(appCheckpointDir);
		final Path expectedSavepointPath = new Path(savepointDir);

		final JobManagerStorage storage = new JobManagerStorage(appCheckpointDir);

		final Configuration config = new Configuration();
		config.setString(CheckpointingOptions.SNAPSHOT_STORAGE, "filesystem"); // check that this is not accidentally picked up
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir); // this parameter should not be picked up
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

		SnapshotStorage loadedStorage = SnapshotStorageLoader
			.fromApplicationOrConfigOrDefault(
				storage,
				null,
				new ModernStateBackend(),
				config,
				cl,
				null);

		assertThat(loadedStorage, instanceOf(JobManagerStorage.class));

		final JobManagerStorage jmStorage = (JobManagerStorage) loadedStorage;
		assertEquals(expectedCheckpointPath, jmStorage.getCheckpointPath());
		assertEquals(expectedSavepointPath, jmStorage.getSavepointPath());
	}

	// ------------------------------------------------------------------------
	//  File System State Backend
	// ------------------------------------------------------------------------

	/**
	 * Validates loading a file system storage with additional parameters from the cluster configuration.
	 */
	@Test
	public void testLoadFileSystemStorage() throws Exception {
		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();
		final Path expectedCheckpointsPath = new Path(checkpointDir);
		final Path expectedSavepointsPath = new Path(savepointDir);
		final MemorySize threshold = MemorySize.parse("900kb");
		final int minWriteBufferSize = 1024;

		// we configure with the explicit string (rather than AbstractStateBackend#X_STATE_BACKEND_NAME)
		// to guard against config-breaking changes of the name
		final Configuration config1 = new Configuration();
		config1.setString(CheckpointingOptions.SNAPSHOT_STORAGE, "filesystem");
		config1.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		config1.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		config1.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, threshold);
		config1.setInteger(CheckpointingOptions.FS_WRITE_BUFFER_SIZE, minWriteBufferSize);

		final Configuration config2 = new Configuration();
		config2.setString(CheckpointingOptions.SNAPSHOT_STORAGE, FileSystemStorageFactory.class.getName());
		config2.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		config2.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		config2.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, threshold);
		config1.setInteger(CheckpointingOptions.FS_WRITE_BUFFER_SIZE, minWriteBufferSize);

		SnapshotStorage storage1 = SnapshotStorageLoader.loadSnapshotStorageFromConfig(config1, cl, null);
		SnapshotStorage storage2 = SnapshotStorageLoader.loadSnapshotStorageFromConfig(config2, cl, null);

		assertThat(storage1, instanceOf(FileSystemStorage.class));
		assertThat(storage2, instanceOf(FileSystemStorage.class));

		FileSystemStorage fs1 = (FileSystemStorage) storage1;
		FileSystemStorage fs2 = (FileSystemStorage) storage2;

		assertEquals(expectedCheckpointsPath, fs1.getCheckpointPath());
		assertEquals(expectedCheckpointsPath, fs2.getCheckpointPath());
		assertEquals(expectedSavepointsPath, fs1.getSavepointPath());
		assertEquals(expectedSavepointsPath, fs2.getSavepointPath());
		assertEquals(threshold.getBytes(), fs1.getMinFileSizeThreshold());
		assertEquals(threshold.getBytes(), fs2.getMinFileSizeThreshold());
		assertEquals(Math.max(threshold.getBytes(), minWriteBufferSize), fs1.getWriteBufferSize());
		assertEquals(Math.max(threshold.getBytes(), minWriteBufferSize), fs2.getWriteBufferSize());
	}

	/**
	 * Validates taking the application-defined file system storage and adding with additional
	 * parameters from the cluster configuration, but giving precedence to application-defined
	 * parameters over configuration-defined parameters.
	 */
	@Test
	public void testLoadFileSystemStateBackendMixed() throws Exception {
		final String appCheckpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();

		final Path expectedCheckpointsPath = new Path(new URI(appCheckpointDir));
		final Path expectedSavepointsPath = new Path(savepointDir);

		final MemorySize threshold = new MemorySize(50);
		final MemorySize writeBufferSize = new MemorySize(60);

		final FileSystemStorage storage = new FileSystemStorage(new URI(appCheckpointDir), threshold, writeBufferSize);

		final Configuration config = new Configuration();
		config.setString(CheckpointingOptions.SNAPSHOT_STORAGE, "jobmanager"); // this should not be picked up
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir); // this should not be picked up
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.parse("20")); // this should not be picked up
		config.setInteger(CheckpointingOptions.FS_WRITE_BUFFER_SIZE, 3000000); // this should not be picked up

		SnapshotStorage loadedStorage = SnapshotStorageLoader
			.fromApplicationOrConfigOrDefault(
				storage,
				null,
				new ModernStateBackend(),
				config,
				cl,
				null);

		assertThat(loadedStorage, instanceOf(FileSystemStorage.class));

		final FileSystemStorage fs = (FileSystemStorage) loadedStorage;
		assertEquals(expectedCheckpointsPath, fs.getCheckpointPath());
		assertEquals(expectedSavepointsPath, fs.getSavepointPath());
		assertEquals(threshold.getBytes(), fs.getMinFileSizeThreshold());
		assertEquals(writeBufferSize.getBytes(), fs.getWriteBufferSize());
	}


	/**
	 * A simple snapshot storage.
	 */
	private static class SimpleSnapshotStorage implements SnapshotStorage {

		Path defaultSavepointLocation;

		@Override
		public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException {
			return null;
		}

		@Override
		public CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException {
			return null;
		}

		@Override
		public void setDefaultSavepointLocation(Path defaultSavepointLocation) {
			this.defaultSavepointLocation = defaultSavepointLocation;
		}

		@Override
		public SnapshotStorage configure(ReadableConfig config, ClassLoader classLoader) throws IllegalConfigurationException {
			return this;
		}
	}

	/**
	 * A modern state backend.
	 */
	private static class ModernStateBackend implements StateBackend {

		@Override
		public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			Environment env, JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups, KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry,
			TtlTimeProvider ttlTimeProvider,
			MetricGroup metricGroup,
			@Nonnull Collection<KeyedStateHandle> stateHandles,
			CloseableRegistry cancelStreamRegistry) {
			throw new RuntimeException();
		}

		@Override
		public OperatorStateBackend createOperatorStateBackend(
			Environment env,
			String operatorIdentifier,
			@Nonnull Collection<OperatorStateHandle> stateHandles,
			CloseableRegistry cancelStreamRegistry) {
			throw new RuntimeException();
		}
	}

	/**
	 * A legacy state backend.
	 */
	private static class LegacyStateBackend implements StateBackend, SnapshotStorage {

		@Override
		public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			Environment env, JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups, KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry,
			TtlTimeProvider ttlTimeProvider,
			MetricGroup metricGroup,
			@Nonnull Collection<KeyedStateHandle> stateHandles,
			CloseableRegistry cancelStreamRegistry) {
			throw new RuntimeException();
		}

		@Override
		public OperatorStateBackend createOperatorStateBackend(
			Environment env,
			String operatorIdentifier,
			@Nonnull Collection<OperatorStateHandle> stateHandles,
			CloseableRegistry cancelStreamRegistry) {
			throw new RuntimeException();
		}

		@Override
		public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException {
			return null;
		}

		@Override
		public CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException {
			return null;
		}

		@Override
		public void setDefaultSavepointLocation(Path defaultSavepointLocation) {

		}

		@Override
		public SnapshotStorage configure(ReadableConfig config, ClassLoader classLoader) throws IllegalConfigurationException {
			return this;
		}
	}

	static final class FailingFactory implements SnapshotStorageFactory<SnapshotStorage> {

		@Override
		public SnapshotStorage createFromConfig(ReadableConfig config, ClassLoader classLoader) throws IOException {
			throw new IOException("fail!");
		}
	}
}
