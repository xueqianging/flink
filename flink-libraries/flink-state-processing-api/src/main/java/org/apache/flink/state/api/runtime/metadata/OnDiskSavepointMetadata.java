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

package org.apache.flink.state.api.runtime.metadata;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.state.api.runtime.SavepointLoader;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;

/**
 * Returns metadata about a savepoint.
 */
@Internal
public class OnDiskSavepointMetadata implements SavepointMetadata {

	private final Savepoint savepoint;

	private int cachedMaxParallelism = Integer.MIN_VALUE;

	public OnDiskSavepointMetadata(String path) throws IOException {
		savepoint = SavepointLoader.loadSavepoint(path);
	}

	/**
	 * @return The max parallelism for the savepoint.
	 */
	@Override
	public int maxParallelism() {
		if (cachedMaxParallelism == Integer.MIN_VALUE) {
			cachedMaxParallelism = savepoint
				.getOperatorStates()
				.stream()
				.map(OperatorState::getMaxParallelism)
				.max(Comparator.naturalOrder())
				.orElseThrow(() -> new RuntimeException("Savepoint's must contain at least one operator"));
		}

		return cachedMaxParallelism;
	}

	/**
	 * @return Masters states for the savepoint.
	 */
	@Override
	public Collection<MasterState> getMasterStates() {
		return savepoint.getMasterStates();
	}

	@Override
	public Collection<OperatorState> getOperatorStates() {
		return savepoint.getOperatorStates();
	}
}
