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

package org.apache.flink.streaming.api.operators.sorted.state;

import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.util.DynamicCodeLoadingException;

import java.io.IOException;

public class BootstrapStateBackendFactory
        implements StateBackendFactory<BatchExecutionStateBackend> {

    private static final String ROCKSDB_STATE_BACKEND_FACTORY =
            "org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackendFactory";

    @Override
    public BatchExecutionStateBackend createFromConfig(
            ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException, IOException {

        try {
            StateBackend persistent = loadPersistent(config, classLoader);
            return new BatchExecutionStateBackend(persistent);
        } catch (Exception e) {
            throw new IOException("failed to load persistent state backend", e);
        }
    }

    private static StateBackend loadPersistent(ReadableConfig config, ClassLoader classLoader)
            throws Exception {
        String backendName = "rocksDB";

        StateBackendFactory<?> factory;
        try {
            @SuppressWarnings("rawtypes")
            Class<? extends StateBackendFactory> clazz =
                    Class.forName(ROCKSDB_STATE_BACKEND_FACTORY, false, classLoader)
                            .asSubclass(StateBackendFactory.class);

            factory = clazz.newInstance();
        } catch (ClassNotFoundException e) {
            throw new DynamicCodeLoadingException(
                    "Cannot find configured state backend factory class: " + backendName, e);
        } catch (ClassCastException | InstantiationException | IllegalAccessException e) {
            throw new DynamicCodeLoadingException(
                    "The class configured under '"
                            + StateBackendOptions.STATE_BACKEND.key()
                            + "' is not a valid state backend factory ("
                            + backendName
                            + ')',
                    e);
        }

        return factory.createFromConfig(config, classLoader);
    }
}
