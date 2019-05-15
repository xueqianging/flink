package org.apache.flink.connectors.savepoint.input;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connectors.savepoint.runtime.OperatorIDGenerator;
import org.apache.flink.connectors.savepoint.runtime.SavepointLoader;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An input format for querying the operators in a {@code Savepoint}.
 */
@Internal
public class OperatorInputFormat
	extends GenericInputFormat<OperatorState>
	implements NonParallelInput,
	ResultTypeQueryable<OperatorState> {

	private final String path;

	private final Set<OperatorID> droppedOperators;

	private transient Iterator<OperatorState> inner;

	public OperatorInputFormat(String path, List<String> droppedOperators) {
		this.path = path;
		this.droppedOperators = droppedOperators
			.stream()
			.map(OperatorIDGenerator::fromUid)
			.collect(Collectors.toSet());
	}

	@Override
	public void open(GenericInputSplit split) throws IOException {
		try {
			inner = SavepointLoader
				.loadSavepoint(path, getRuntimeContext().getUserCodeClassLoader())
				.getOperatorStates()
				.stream()
				.filter(state -> !droppedOperators.contains(state.getOperatorID()))
				.iterator();
		} catch (IOException e) {
			throw new IOException("Failed to load operators from existing savepoint", e);
		}
	}

	@Override
	public boolean reachedEnd() {
		return !inner.hasNext();
	}

	@Override
	public OperatorState nextRecord(OperatorState reuse) {
		return inner.next();
	}

	@Override
	public TypeInformation<OperatorState> getProducedType() {
		return TypeInformation.of(OperatorState.class);
	}
}

