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

package org.apache.flink.connectors.savepoint.output.metadata;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.savepoint.runtime.SavepointLoader;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;

/**
 * Supplies metadata based off an existing savepoint.
 *
 * <p><b>IMPORTANT:</b> The savepoint must not be loaded on the cluster to
 * ensure all filesystem's are on the classpath.
 */
@Internal
public class OnDiskSavepointMetadataProvider implements SavepointMetadataProvider {

	private final String path;

	private transient Savepoint savepoint;

	public OnDiskSavepointMetadataProvider(String path) {
		this.path = path;
	}

	@Override
	public int maxParallelism() {
		ensureInitialized();

		return savepoint
			.getOperatorStates()
			.stream()
			.map(OperatorState::getMaxParallelism)
			.max(Comparator.naturalOrder())
			.orElseThrow(() -> new RuntimeException("Savepoint's must contain at least one operator"));
	}

	@Override
	public Collection<MasterState> getMasterStates() {
		ensureInitialized();

		return savepoint.getMasterStates();
	}

	private void ensureInitialized() throws RuntimeException {
		if (savepoint != null) {
			return;
		}

		try {
			savepoint = SavepointLoader.loadSavepoint(path, getClass().getClassLoader());
		} catch (IOException exception) {
			throw new FlinkRuntimeException(exception);
		}
	}
}
