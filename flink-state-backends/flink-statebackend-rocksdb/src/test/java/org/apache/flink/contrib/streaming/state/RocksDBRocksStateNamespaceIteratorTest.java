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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.iterator.RocksStateNamespaceIterator;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static org.mockito.Mockito.mock;

/**
 * Tests for the RocksIteratorWrapper.
 */
public class RocksDBRocksStateNamespaceIteratorTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void testIterator() throws Exception{

		// test for keyGroupPrefixBytes == 1 && ambiguousKeyPossible == false
		testIteratorHelper(StringSerializer.INSTANCE, IntSerializer.INSTANCE, 128, i -> i);

		// test for keyGroupPrefixBytes == 1 && ambiguousKeyPossible == true
		testIteratorHelper(StringSerializer.INSTANCE, StringSerializer.INSTANCE, 128, i -> String.valueOf(i));

		// test for keyGroupPrefixBytes == 2 && ambiguousKeyPossible == false
		testIteratorHelper(StringSerializer.INSTANCE, IntSerializer.INSTANCE, 256, i -> i);

		// test for keyGroupPrefixBytes == 2 && ambiguousKeyPossible == true
		testIteratorHelper(StringSerializer.INSTANCE, StringSerializer.INSTANCE, 256, i -> String.valueOf(i));
	}

	<N> void testIteratorHelper(
		TypeSerializer<String> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		int maxKeyGroupNumber,
		Function<Integer, N> getNamespaceFunc) throws Exception {

		String testStateName = "aha";
		String key = "key";

		String dbPath = tmp.newFolder().getAbsolutePath();
		String checkpointPath = tmp.newFolder().toURI().toString();
		RocksDBStateBackend backend = new RocksDBStateBackend(new FsStateBackend(checkpointPath), true);
		backend.setDbStoragePath(dbPath);

		Environment env = new DummyEnvironment("TestTask", 1, 0);
		RocksDBKeyedStateBackend<String> keyedStateBackend = (RocksDBKeyedStateBackend<String>) backend.createKeyedStateBackend(
			env,
			new JobID(),
			"Test",
			keySerializer,
			maxKeyGroupNumber,
			new KeyGroupRange(0, maxKeyGroupNumber - 1),
			mock(TaskKvStateRegistry.class),
			TtlTimeProvider.DEFAULT,
			new UnregisteredMetricsGroup(),
			Collections.emptyList(),
			new CloseableRegistry());

		try {
			for (int i = 0; i < 1000; ++i) {
				ValueState<String> testState = keyedStateBackend.getPartitionedState(
					getNamespaceFunc.apply(i),
					namespaceSerializer,
					new ValueStateDescriptor<>(testStateName, String.class));

				keyedStateBackend.setCurrentKey(key);
				testState.update(String.valueOf(i));
			}
		} catch (Exception e) {
			throw new RuntimeException("Failed to seed state backend", e);
		}

		// already created with the state, should be closed with the backend
		ColumnFamilyHandle handle = keyedStateBackend.getColumnFamilyHandle(testStateName);

		boolean ambiguousKeyPossible = RocksDBKeySerializationUtils.isAmbiguousKeyPossible(keySerializer, namespaceSerializer);
		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, maxKeyGroupNumber);

		try (
			RocksIteratorWrapper iterator = RocksDBOperationUtils.getRocksIterator(keyedStateBackend.db, handle);
			RocksStateNamespaceIterator<String, N> iteratorWrapper = new RocksStateNamespaceIterator<>(
				iterator,
				testStateName,
				key,
				keySerializer,
				namespaceSerializer,
				ambiguousKeyPossible,
				keyGroup,
				keyedStateBackend.getKeyGroupPrefixBytes()
			)) {

			// valid record
			List<Integer> fetchedNamespaces = new ArrayList<>(1000);
			while (iteratorWrapper.hasNext()) {
				fetchedNamespaces.add(Integer.parseInt(iteratorWrapper.next().toString()));
			}

			fetchedNamespaces.sort(Comparator.comparingInt(a -> a));
			Assert.assertEquals(1000, fetchedNamespaces.size());

			for (int i = 0; i < 1000; ++i) {
				Assert.assertEquals(i, fetchedNamespaces.get(i).intValue());
			}
		} finally {
			if (keyedStateBackend != null) {
				keyedStateBackend.dispose();
			}
		}
	}
}
