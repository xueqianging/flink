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

package org.apache.flink.connectors.savepoint.output;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.savepoint.runtime.SavepointLoader;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.Comparator;

/**
 * An supplier that returns the max parallelism
 * of an existing savepoint.
 *
 * <b>IMPORTANT:</b> The savepoint must not be loaded on the cluster to
 * ensure all filesystem's are on the classpath.
 */
@Internal
public class OnDiskMaxParallelismSupplier implements MaxParallelismSupplier {

	private final String path;

	public OnDiskMaxParallelismSupplier(String path) {
		this.path = path;
	}

	@Override
	public Integer get() {
		try {
			return SavepointLoader
				.loadSavepoint(path, this.getClass().getClassLoader())
				.getOperatorStates()
				.stream()
				.map(OperatorState::getMaxParallelism)
				.max(Comparator.naturalOrder())
				.orElseThrow(() -> new RuntimeException("Savepoint's must contain at least one operator"));
		} catch (IOException e) {
			throw new FlinkRuntimeException("Failed to calculate max parallelism from existing savepoint", e);
		}
	}
}
