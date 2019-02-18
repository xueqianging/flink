package org.apache.flink.connectors.savepoint.input;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;

import java.util.Iterator;
import java.util.List;

/**
 * An iterator for reading all keys in a state backend across multiple partitioned states.
 *
 * <p>To read unique keys across all partitioned states callers must invoke {@link MultiStateKeyIterator#remove}.
 *
 * @param <K> Type of the key by which state is keyed.
 */
final class MultiStateKeyIterator<K> implements Iterator<K> {
	private final List<? extends StateDescriptor<?, ?>> descriptors;

	private final AbstractKeyedStateBackend<K> backend;

	private final Iterator<K> internal;

	private K currentKey;

	MultiStateKeyIterator(List<? extends StateDescriptor<?, ?>> descriptors, AbstractKeyedStateBackend<K> backend) {
		this.descriptors = descriptors;

		this.backend = backend;

		this.internal = descriptors.stream()
			.flatMap(descriptor -> backend.getKeys(descriptor.getName(), VoidNamespace.INSTANCE))
			.iterator();
	}

	@Override
	public boolean hasNext() {
		return internal.hasNext();
	}

	@Override
	public K next() {
		currentKey = internal.next();
		return currentKey;
	}

	/**
	 * Removes the current key from <b>ALL</b> known states
	 * in the state backend.
	 */
	@Override
	public void remove() {
		if (currentKey != null) {
			for (StateDescriptor<?, ?> descriptor : descriptors) {
				try {
					State state = backend.getPartitionedState(
						VoidNamespace.INSTANCE,
						VoidNamespaceSerializer.INSTANCE,
						descriptor
					);

					state.clear();
				} catch (Exception e) {
					throw new RuntimeException("Failed to remove partitioned state from state backend", e);
				}
			}
		}
	}
}
