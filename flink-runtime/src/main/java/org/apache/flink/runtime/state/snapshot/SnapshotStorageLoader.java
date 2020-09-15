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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.snapshot.factory.FileSystemStorageFactory;
import org.apache.flink.runtime.state.snapshot.factory.JobManagerStorageFactory;
import org.apache.flink.runtime.state.snapshot.factory.SnapshotStorageFactory;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class contains utility methods to load snapshot storages from configurations.
 */
public class SnapshotStorageLoader {

	// ------------------------------------------------------------------------
	//  Configuration shortcut names
	// ------------------------------------------------------------------------

	/** The shortcut configuration name for the JobManagerStorage. */
	public static final String JOBMANAGER_STORAGE = "jobmanager";

	/** The shortcut configuration name for the FileSystemStorage. */
	public static final String FS_STORAGE = "filesystem";

	// ------------------------------------------------------------------------
	//  Loading the snapshot storage from a configuration
	// ------------------------------------------------------------------------

	/**
	 * Loads the snapshot storage from the configuration, from the parameter 'state.backend', as defined
	 * in {@link CheckpointingOptions#SNAPSHOT_STORAGE}.
	 *
	 * <p>The snapshot storages can be specified either via their shortcut name, or via the class name
	 * of a {@link SnapshotStorageFactory}. If a StateBackendFactory class name is specified, the factory
	 * is instantiated (via its zero-argument constructor) and its
	 * {@link SnapshotStorageFactory#createFromConfig(ReadableConfig, ClassLoader)} method is called.
	 *
	 * @param config The configuration to load the snapshot storage from
	 * @param classLoader The class loader that should be used to load the snapshot storage
	 * @param logger Optionally, a logger to log actions to (may be null)
	 *
	 * @return The instantiated snapshot storage.
	 *
	 * @throws DynamicCodeLoadingException
	 *             Thrown if a snapshot storage factory is configured and the factory class was not
	 *             found or the factory could not be instantiated
	 * @throws IllegalConfigurationException
	 *             May be thrown by the SnapshotStorageFactory when creating / configuring the state
	 *             backend in the factory
	 * @throws IOException
	 *             May be thrown by the SnapshotStorageFactory when instantiating the snapshot storage
	 */
	public static SnapshotStorage loadSnapshotStorageFromConfig(
		ReadableConfig config,
		ClassLoader classLoader,
		@Nullable Logger logger) throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {

		checkNotNull(config, "config");
		checkNotNull(classLoader, "classLoader");

		final String storageName = config.get(CheckpointingOptions.SNAPSHOT_STORAGE);
		if (storageName == null) {
			return null;
		}

		if (logger != null) {
			logger.info("Loading snapshot storage via factory {}", storageName);
		}

		if (JOBMANAGER_STORAGE.equalsIgnoreCase(storageName)) {
			return new JobManagerStorageFactory().createFromConfig(config, classLoader);
		}

		if (FS_STORAGE.equalsIgnoreCase(storageName)) {
			return new FileSystemStorageFactory().createFromConfig(config, classLoader);
		}

		SnapshotStorageFactory<?> factory;
		try {
			@SuppressWarnings("rawtypes")
			Class<? extends SnapshotStorageFactory> clazz =
				Class.forName(storageName, false, classLoader)
					.asSubclass(SnapshotStorageFactory.class);

			factory = clazz.newInstance();
		}
		catch (ClassNotFoundException e) {
			throw new DynamicCodeLoadingException(
				"Cannot find configured snapshot storage factory class: " + storageName, e);
		}
		catch (ClassCastException | InstantiationException | IllegalAccessException e) {
			throw new DynamicCodeLoadingException("The class configured under '" +
				CheckpointingOptions.SNAPSHOT_STORAGE.key() + "' is not a valid snapshot storage factory (" +
				storageName + ')', e);
		}

		return factory.createFromConfig(config, classLoader);
	}

	/**
	 * Checks if an application-defined snapshot storage is given, and if not, loads the
	 * storage from the configuration, from the parameter 'state.snapshot-storage'.
	 * If no snapshot storage is configured, and no checkpoint directory is set,
	 * this instantiates the default storage (the {@code JobManagerSnapshotStorage}).
	 *
	 * <p>If the configured state backend implements {@link SnapshotStorage}
	 * then that will be used.
	 *
	 * <p>If an application-defined storage is found
	 * this methods calls {@link SnapshotStorage#configure(ReadableConfig, ClassLoader)}
	 * on the snapshot.
	 *
	 * <p>Refer to {@link #loadSnapshotStorageFromConfig(ReadableConfig, ClassLoader, Logger)} for details on
	 * how the snapshot is loaded from the configuration.
	 *
	 * @param config The configuration to load the snapshot storage from
	 * @param classLoader The class loader that should be used to load the snapshot storage
	 * @param logger Optionally, a logger to log actions to (may be null)
	 *
	 * @return The instantiated snapshot storage.
	 *
	 * @throws DynamicCodeLoadingException
	 *             Thrown if a snapshot factory is configured and the factory class was not
	 *             found or the factory could not be instantiated
	 * @throws IllegalConfigurationException
	 *             May be thrown by the SnapshotLoaderFactory when creating / configuring the state
	 *             backend in the factory
	 * @throws IOException
	 *             May be thrown by the SnapshotStorageFactory when instantiating the snapshot storage
	 */
	public static SnapshotStorage fromApplicationOrConfigOrDefault(
		SnapshotStorage fromApplication,
		@Nullable Path appDefaultSavepointLocation,
		StateBackend stateBackend,
		Configuration config,
		ClassLoader classLoader,
		@Nullable Logger logger) throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {

		checkNotNull(stateBackend, "statebackend");
		checkNotNull(config, "config");
		checkNotNull(classLoader, "classLoader");

		// Legacy state backends always have precedence.
		if (stateBackend instanceof SnapshotStorage) {
			if (logger != null) {
				logger.warn("Using a legacy state backend as snapshot storage");
			}

			return (SnapshotStorage) stateBackend;
		}

		SnapshotStorage storage;
		if (fromApplication != null) {
			if (logger != null) {
				logger.info("Using application-defined snapshot storage: {}", fromApplication);
				logger.info("Using job/cluster config to configure application-defined snapshot storage: {}", fromApplication);
			}

			storage = fromApplication.configure(config, classLoader);
		} else {
			SnapshotStorage fromConfig = loadSnapshotStorageFromConfig(config, classLoader, logger);
			if (fromConfig != null) {
				storage = fromConfig;
			} else if (config.contains(CheckpointingOptions.CHECKPOINTS_DIRECTORY)) {
				storage = new FileSystemStorageFactory().createFromConfig(config, classLoader);
				if (logger != null) {
					logger.info("No snapshot storage has been configured, but a checkpoint directory " +
						"has been provided. Using FileSystem storage {}", storage);
				}
			} else {
				storage = new JobManagerStorageFactory().createFromConfig(config, classLoader);
				if (logger != null) {
					logger.info("No snapshot storage has been configured, using default JobManager {}", storage);
				}
			}
		}

		Path defaultSavepointDir = null;
		if (appDefaultSavepointLocation != null) {
			defaultSavepointDir = appDefaultSavepointLocation;
		} else if (config.contains(CheckpointingOptions.SAVEPOINT_DIRECTORY)) {
			String path = config.get(CheckpointingOptions.SAVEPOINT_DIRECTORY);
			defaultSavepointDir = new Path(path);
		}

		if (defaultSavepointDir != null) {
			storage.setDefaultSavepointLocation(defaultSavepointDir);
		}

		return storage;
	}

	// ------------------------------------------------------------------------

	/** This class is not meant to be instantiated */
	private SnapshotStorageLoader() {}
}
