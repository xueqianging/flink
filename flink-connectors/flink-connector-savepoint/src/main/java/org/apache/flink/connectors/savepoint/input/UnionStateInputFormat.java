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

package org.apache.flink.connectors.savepoint.input;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.connectors.savepoint.input.splits.OperatorStateInputSplit;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.OperatorStateBackend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Input format for reading operator union state.
 *
 * @param <O> The generic type of the state.
 */
@PublicEvolving
public class UnionStateInputFormat<O> extends OperatorStateInputFormat<O> {
	private final ListStateDescriptor<O> descriptor;

	/**
	 * Creates an input format for reading union state from an operator in a savepoint.
	 *
	 * @param savepointPath The path to an existing savepoint.
	 * @param uid The uid of a particular operator.
	 * @param descriptor The descriptor for this state, providing a name and serializer.
	 */
	public UnionStateInputFormat(String savepointPath, String uid, ListStateDescriptor<O> descriptor) {
		super(savepointPath, uid);
		this.descriptor = descriptor;
	}

	@Override
	public OperatorStateInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		OperatorStateInputSplit[] splits = super.createInputSplits(minNumSplits);
		if (splits == null || splits.length == 0) {
			return splits;
		}

		// We only want to output a single instance of the union state so we only need
		// to process a single input split. An arbitrary split is chosen and
		// sub-partitioned for better data distribution across the cluster.
		AtomicInteger splitNumber = new AtomicInteger(0);

		return partition(
			splits[0].getPrioritizedManagedOperatorState().get(0).asList(),
			minNumSplits
		).stream().map(state ->
			new OperatorStateInputSplit(new StateObjectCollection<>(new ArrayList<>(state)), splitNumber.getAndIncrement())
		).toArray(OperatorStateInputSplit[]::new);
	}

	@Override
	protected final Iterable<O> getElements(OperatorStateBackend restoredBackend) throws Exception {
		return restoredBackend.getUnionListState(descriptor).get();
	}

	/**
	 * Partition a list into approximately n buckets.
	 */
	private static <T> Collection<List<T>> partition(List<T> elements, int numBuckets) {
		Map<Integer, List<T>> buckets = new HashMap<>(numBuckets);

		for (int i = 0; i < elements.size(); i++) {
			buckets.computeIfAbsent(
				i % numBuckets,
				key -> new ArrayList<>(elements.size() / numBuckets)
			).add(elements.get(i));
		}

		return buckets.values();
	}
}
