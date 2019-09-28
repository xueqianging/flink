/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.iterator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBKeySerializationUtils;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Adapter class to bridge between {@link RocksIteratorWrapper} and {@link Iterator} to iterate over
 * the keys and namespaces. This class is not thread safe.
 *
 * @param <K> the type of the iterated objects, which are keys in RocksDB.
 * @param <N> the type of the iterated object, which are namespaces.
 */
public class RocksStateKeyNamespaceIterator<K, N> implements Iterator<Tuple2<K, N>>, AutoCloseable {

	@Nonnull
	private final RocksIteratorWrapper iterator;

	@Nonnull
	private final String state;

	@Nonnull
	private final TypeSerializer<K> keySerializer;

	@Nonnull
	private final TypeSerializer<N> namespaceSerializer;

	private final boolean ambiguousKeyPossible;
	private final int keyGroupPrefixBytes;
	private final DataInputDeserializer byteArrayDataInputView;
	private Tuple2<K, N> nextKey;

	public RocksStateKeyNamespaceIterator(
		@Nonnull RocksIteratorWrapper iterator,
		@Nonnull String state,
		@Nonnull TypeSerializer<K> keySerializer,
		@Nonnull TypeSerializer<N> namespaceSerializer,
		int keyGroupPrefixBytes,
		boolean ambiguousKeyPossible) {
		this.iterator = iterator;
		this.state = state;
		this.keySerializer = keySerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
		this.nextKey = null;
		this.ambiguousKeyPossible = ambiguousKeyPossible;
		this.byteArrayDataInputView = new DataInputDeserializer();
	}

	@Override
	public boolean hasNext() {
		try {
			while (nextKey == null && iterator.isValid()) {

				final byte[] keyBytes = iterator.key();
				nextKey = deserializeKeyAndNamespace(keyBytes, byteArrayDataInputView);

				iterator.next();
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Failed to access state [" + state + "]", e);
		}
		return nextKey != null;
	}

	@Override
	public Tuple2<K, N> next() {
		if (!hasNext()) {
			throw new NoSuchElementException("Failed to access state [" + state + "]");
		}

		Tuple2<K, N> tmpKey = nextKey;
		nextKey = null;
		return tmpKey;
	}

	private Tuple2<K, N> deserializeKeyAndNamespace(byte[] keyBytes, DataInputDeserializer readView) throws IOException {
		readView.setBuffer(keyBytes, keyGroupPrefixBytes, keyBytes.length - keyGroupPrefixBytes);
		K key = RocksDBKeySerializationUtils.readKey(
			keySerializer,
			byteArrayDataInputView,
			ambiguousKeyPossible);

		N namespace = RocksDBKeySerializationUtils.readNamespace(
			namespaceSerializer,
			byteArrayDataInputView,
			ambiguousKeyPossible
		);

		return Tuple2.of(key, namespace);
	}

	@Override
	public void close() {
		iterator.close();
	}
}
