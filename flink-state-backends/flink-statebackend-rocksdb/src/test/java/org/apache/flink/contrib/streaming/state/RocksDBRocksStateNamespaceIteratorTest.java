package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.iterator.RocksStateKeysIterator;
import org.apache.flink.contrib.streaming.state.iterator.RocksStateNamespaceIterator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyHandle;

import java.util.ArrayList;
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
		testIteratorHelper(IntSerializer.INSTANCE, StringSerializer.INSTANCE, 128, 1);

		// test for keyGroupPrefixBytes == 1 && ambiguousKeyPossible == true
		testIteratorHelper(StringSerializer.INSTANCE, StringSerializer.INSTANCE, 128, "1");

		// test for keyGroupPrefixBytes == 2 && ambiguousKeyPossible == false
		testIteratorHelper(IntSerializer.INSTANCE, StringSerializer.INSTANCE, 256, 1);

		// test for keyGroupPrefixBytes == 2 && ambiguousKeyPossible == true
		testIteratorHelper(StringSerializer.INSTANCE, StringSerializer.INSTANCE, 256, "1");
	}

	private <K> void testIteratorHelper(
		TypeSerializer<K> keySerializer,
		TypeSerializer namespaceSerializer,
		int maxKeyGroupNumber,
		K key) throws Exception {

		String testStateName = "aha";

		String dbPath = tmp.newFolder().getAbsolutePath();
		String checkpointPath = tmp.newFolder().toURI().toString();
		RocksDBStateBackend backend = new RocksDBStateBackend(new FsStateBackend(checkpointPath), true);
		backend.setDbStoragePath(dbPath);

		Environment env = new DummyEnvironment("TestTask", 1, 0);
		RocksDBKeyedStateBackend<K> keyedStateBackend = (RocksDBKeyedStateBackend<K>) backend.createKeyedStateBackend(
			env,
			new JobID(),
			"Test",
			keySerializer,
			maxKeyGroupNumber,
			new KeyGroupRange(0, maxKeyGroupNumber - 1),
			mock(TaskKvStateRegistry.class));

		try {
			keyedStateBackend.restore(null);

			for (int i = 0; i < 1000; ++i) {
				ValueState<String> testState = keyedStateBackend.getPartitionedState(
					Integer.toString(i),
					namespaceSerializer,
					new ValueStateDescriptor<>(testStateName, String.class));

				keyedStateBackend.setCurrentKey(key);
				testState.update(String.valueOf(i));
			}

			boolean ambiguousKeyPossible = RocksDBKeySerializationUtils.isAmbiguousKeyPossible(keySerializer, namespaceSerializer);

			try (
				ColumnFamilyHandle handle = keyedStateBackend.getColumnFamilyHandle(testStateName);
				RocksIteratorWrapper iterator = RocksDBKeyedStateBackend.getRocksIterator(keyedStateBackend.db, handle);
				RocksStateNamespaceIterator<K, String> iteratorWrapper =
					new RocksStateNamespaceIterator<K, String>(
						iterator,
						testStateName,
						KeyGroupRangeAssignment.assignToKeyGroup(key, maxKeyGroupNumber - 1),
						keySerializer,
						key,
						namespaceSerializer,
						keyedStateBackend.getKeyGroupPrefixBytes(),
						ambiguousKeyPossible)) {

				// valid record
				List<Integer> fetchedNamespaces = new ArrayList<>(1000);
				while (iteratorWrapper.hasNext()) {
					fetchedNamespaces.add(Integer.parseInt(iteratorWrapper.next()));
				}

				fetchedNamespaces.sort(Comparator.comparingInt(a -> a));
				Assert.assertEquals(1000, fetchedNamespaces.size());

				for (int i = 0; i < 1000; ++i) {
					Assert.assertEquals(i, fetchedNamespaces.get(i).intValue());
				}
			}
		} finally {
			if (keyedStateBackend != null) {
				keyedStateBackend.dispose();
			}
		}
	}
}
