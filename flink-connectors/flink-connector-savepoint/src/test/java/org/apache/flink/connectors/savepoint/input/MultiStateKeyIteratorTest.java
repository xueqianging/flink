package org.apache.flink.connectors.savepoint.input;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.ttl.mock.MockStateBackend;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link MultiStateKeyIterator}.
 */
public class MultiStateKeyIteratorTest {
	private static final List<ValueStateDescriptor<Integer>> descriptors;

	static {
		descriptors = new ArrayList<>(2);
		descriptors.add(new ValueStateDescriptor<>("state-1", Types.INT));
		descriptors.add(new ValueStateDescriptor<>("state-2", Types.INT));
	}

	private static AbstractKeyedStateBackend<Integer> createKeyedStateBackend() throws Exception{
		MockStateBackend backend = new MockStateBackend();

		return backend.createKeyedStateBackend(
			new DummyEnvironment(),
			new JobID(),
			"mock-backend",
			IntSerializer.INSTANCE,
			129,
			KeyGroupRange.of(0, 128),
			new KvStateRegistry().createTaskRegistry(new JobID(), new JobVertexID()));
	}

	private static void setKey(AbstractKeyedStateBackend<Integer> backend, ValueStateDescriptor<Integer> descriptor, int key) throws Exception{
		backend.setCurrentKey(key);
		backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor).update(0);
	}

	@Test
	public void testIteratorPullsKeyFromAllDescriptors() throws Exception {
		AbstractKeyedStateBackend<Integer> keyedStateBackend = createKeyedStateBackend();

		setKey(keyedStateBackend, descriptors.get(0), 1);
		setKey(keyedStateBackend, descriptors.get(1), 2);

		MultiStateKeyIterator<Integer> iterator = new MultiStateKeyIterator<>(descriptors, keyedStateBackend);

		List<Integer> keys = new ArrayList<>();

		while (iterator.hasNext()) {
			keys.add(iterator.next());
		}

		Assert.assertEquals("Unexpected number of keys", 2, keys.size());
		Assert.assertEquals("Unexpected keys found", Arrays.asList(1, 2), keys);
	}

	@Test
	public void testIteratorRemovesFromAllDescriptors() throws Exception {
		AbstractKeyedStateBackend<Integer> keyedStateBackend = createKeyedStateBackend();

		setKey(keyedStateBackend, descriptors.get(0), 1);
		setKey(keyedStateBackend, descriptors.get(1), 1);

		MultiStateKeyIterator<Integer> iterator = new MultiStateKeyIterator<>(descriptors, keyedStateBackend);

		int key = iterator.next();
		Assert.assertEquals("Unexpected keys pulled from state backend", 1, key);

		iterator.remove();
		Assert.assertFalse("Failed to remove key from all descriptors in state backend", iterator.hasNext());

		for (StateDescriptor<?, ?> descriptor : descriptors) {
			Assert.assertEquals(
				"Failed to remove key for state descriptor",
				0,
				keyedStateBackend.getKeys(descriptor.getName(), VoidNamespace.INSTANCE).count());
		}
	}
}
