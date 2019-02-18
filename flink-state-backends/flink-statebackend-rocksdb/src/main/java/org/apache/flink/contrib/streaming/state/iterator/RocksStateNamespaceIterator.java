package org.apache.flink.contrib.streaming.state.iterator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBKeySerializationUtils;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Adapter class to bridge between {@link RocksIteratorWrapper} and {@link Iterator} to iterate over the keys. This class
 * is not thread safe.
 *
 * @param <K> the type of the iterated objects, which are keys in RocksDB.
 */
public class RocksStateNamespaceIterator<K, N> implements Iterator<N>, AutoCloseable {

	@Nonnull
	private final RocksIteratorWrapper iterator;

	@Nonnull
	private final String state;

	@Nonnull
	public final TypeSerializer<K> keySerializer;

	@Nonnull
	public final K key;

	@Nonnull
	private final TypeSerializer<N> namespaceSerializer;

	@Nonnull
	private final byte[] keyPrefixBytes;

	private final boolean ambiguousKeyPossible;
	private final int keyGroupPrefixBytes;
	private final DataInputDeserializer byteArrayDataInputView;
	private N nextKey;
	private N previousKey;

	public RocksStateNamespaceIterator(
		@Nonnull RocksIteratorWrapper iterator,
		@Nonnull String state,
		@Nonnegative int keyGroup,
		@Nonnull TypeSerializer<K> keySerializer,
		@Nonnull K key,
		@Nonnull TypeSerializer<N> namespaceSerializer,
		int keyGroupPrefixBytes,
		boolean ambiguousKeyPossible) {

		this.iterator = iterator;
		this.state = state;
		this.keySerializer = keySerializer;
		this.key = key;
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
		this.namespaceSerializer = namespaceSerializer;
		this.nextKey = null;
		this.previousKey = null;
		this.ambiguousKeyPossible = ambiguousKeyPossible;
		this.byteArrayDataInputView = new DataInputDeserializer();

		try {
			DataOutputSerializer serializer = new DataOutputSerializer(128);

			RocksDBKeySerializationUtils.writeKeyGroup(keyGroup, keyGroupPrefixBytes, serializer);
			keySerializer.serialize(key, serializer);

			keyPrefixBytes = serializer.getCopyOfBuffer();
		} catch (Exception e) {
			throw new FlinkRuntimeException("Failed to initialize namespace iterator", e);
		}

		iterator.seek(keyPrefixBytes);
	}

	@Override
	public boolean hasNext() {
		try {
			while (nextKey == null && iterator.isValid()) {

				final byte[] keyBytes = iterator.key();
				if (startWithKeyPrefix(keyPrefixBytes, keyBytes)) {
					final N namespace = deserializeNamespace(keyBytes, byteArrayDataInputView);

					if (!Objects.equals(previousKey, namespace)) {
						nextKey = namespace;
						previousKey = namespace;
					}
				}

				iterator.next();
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Failed to access state [" + state + "]", e);
		}

		return nextKey != null;
	}

	@Override
	public N next() {
		if (!hasNext()) {
			throw new NoSuchElementException("Failed to access state [" + state + "]");
		}

		N tmpKey = nextKey;
		nextKey = null;
		return tmpKey;
	}

	private N deserializeNamespace(byte[] keyBytes, DataInputDeserializer readView) throws IOException {
		readView.setBuffer(keyBytes, keyPrefixBytes.length, keyBytes.length - keyPrefixBytes.length);
		return RocksDBKeySerializationUtils.readNamespace(
			namespaceSerializer,
			byteArrayDataInputView,
			ambiguousKeyPossible);
	}

	private boolean startWithKeyPrefix(byte[] keyPrefixBytes, byte[] rawKeyBytes) {
		if (rawKeyBytes.length < keyPrefixBytes.length) {
			return false;
		}

		for (int i = keyPrefixBytes.length; --i >= keyGroupPrefixBytes; ) {
			if (rawKeyBytes[i] != keyPrefixBytes[i]) {
				return false;
			}
		}

		return true;
	}

	@Override
	public void close() {
		iterator.close();
	}
}
