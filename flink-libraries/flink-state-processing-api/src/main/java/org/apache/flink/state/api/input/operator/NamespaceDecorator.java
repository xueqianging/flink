package org.apache.flink.state.api.input.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;

/**
 * Decorates each key with a static namespace.
 */
@Internal
public class NamespaceDecorator<KEY, N> implements Iterator<Tuple2<KEY, N>> {

	private final Iterator<KEY> keys;

	private final N namespace;

	private final TypeSerializer<N> namespaceSerializer;

	public NamespaceDecorator(Iterator<KEY> keys, N namespace, TypeSerializer<N> namespaceSerializer) {
		this.keys = Preconditions.checkNotNull(keys, "keys cannot be null");
		this.namespace = Preconditions.checkNotNull(namespace, "namespace cannot be null");
		this.namespaceSerializer = Preconditions.checkNotNull(namespaceSerializer, "Serializer cannot be null");
	}

	@Override
	public boolean hasNext() {
		return keys.hasNext();
	}

	@Override
	public Tuple2<KEY, N> next() {
		KEY key = keys.next();
		return Tuple2.of(key, namespaceSerializer.copy(namespace));
	}

	@Override
	public void remove() {
		keys.remove();
	}
}
