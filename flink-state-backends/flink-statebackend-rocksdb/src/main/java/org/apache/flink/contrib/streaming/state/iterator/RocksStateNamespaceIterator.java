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

package org.apache.flink.contrib.streaming.state.iterator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBKeySerializationUtils;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Adapter class to bridge between {@link RocksIteratorWrapper} and {@link Iterator} to iterate over the namespaces
 * for a key. This class is not thread safe.
 *
 * @param <K> the type of the iterated objects, which are keys in RocksDB.
 */
public class RocksStateNamespaceIterator<K, N> implements Iterator<N>, AutoCloseable {

	@Nonnull
	private final RocksIteratorWrapper iterator;

	@Nonnull
	private final String state;

	@Nonnull
	private final TypeSerializer<N> namespaceSerializer;

	private final boolean ambiguousKeyPossible;

	private final DataInputDeserializer byteArrayDataInputView;

	private final byte[] prefix;

	private N nextNamespace;

	public RocksStateNamespaceIterator(
		@Nonnull RocksIteratorWrapper iterator,
		@Nonnull String state,
		@Nonnull K key,
		@Nonnull TypeSerializer<K> keySerializer,
		@Nonnull TypeSerializer<N> namespaceSerializer,
		boolean ambiguousKeyPossible,
		int keyGroup,
		int keyGroupPrefixBytes) {
		this.iterator = iterator;
		this.state = state;
		this.namespaceSerializer = namespaceSerializer;
		this.ambiguousKeyPossible = ambiguousKeyPossible;

		this.byteArrayDataInputView = new DataInputDeserializer();

		try {
			DataOutputSerializer keyOutView = new DataOutputSerializer(128);

			RocksDBKeySerializationUtils.writeKeyGroup(
				keyGroup,
				keyGroupPrefixBytes,
				keyOutView);

			RocksDBKeySerializationUtils.writeKey(
				key,
				keySerializer,
				keyOutView,
				ambiguousKeyPossible
			);

			this.prefix = keyOutView.getCopyOfBuffer();
			iterator.seek(prefix);
		} catch (IOException shouldNeverHappen) {
			throw new FlinkRuntimeException(shouldNeverHappen);
		}
	}

	@Override
	public boolean hasNext() {
		try {
			while (nextNamespace == null && iterator.isValid()) {
				final byte[] keyBytes = iterator.key();
				byteArrayDataInputView.setBuffer(keyBytes);

				if (isMatchingKey(keyBytes)) {
					byteArrayDataInputView.skipBytesToRead(prefix.length);
					nextNamespace = RocksDBKeySerializationUtils.readNamespace(
						namespaceSerializer,
						byteArrayDataInputView,
						ambiguousKeyPossible
					);
				}

				iterator.next();
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Failed to access state [" + state + "]", e);
		}

		return nextNamespace != null;
	}

	private boolean isMatchingKey(byte[] keyBytes) {
		for (int i = 0; i < prefix.length; i++) {
			if (keyBytes[i] != prefix[i]) {
				return false;
			}
		}
		return true;
	}

	@Override
	public N next() {
		if (!hasNext()) {
			throw new NoSuchElementException("Failed to access state [" + state + "]");
		}

		N tmpKey = nextNamespace;
		nextNamespace = null;
		return tmpKey;
	}

	@Override
	public void close() {
		iterator.close();
	}
}
