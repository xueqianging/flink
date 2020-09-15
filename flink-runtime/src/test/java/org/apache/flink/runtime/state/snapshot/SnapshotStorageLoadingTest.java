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
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.snapshot.factory.SnapshotStorageFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.DynamicCodeLoadingException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import java.io.IOException;
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

		assertThat("wrong snapshot storage loaded", storage, instanceOf(MemoryStateBackend.class));
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

		assertThat("wrong snapshot storage loaded", storage, instanceOf(FsStateBackend.class));
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
			SnapshotStorage storage = SnapshotStorageLoader
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
			SnapshotStorage storage = SnapshotStorageLoader
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
