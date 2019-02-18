package org.apache.flink.connectors.savepoint.input;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.OperatorStateBackend;

import java.util.Map;

/**
 * The input format for reading {@link org.apache.flink.api.common.state.BroadcastState}.
 *
 * @param <K> The type of the keys in the {@code BroadcastState}.
 * @param <V> The type of the values in the {@code BroadcastState}.
 */
@PublicEvolving
public class BroadcastStateInputFormat<K, V> extends OperatorStateInputFormat<Map.Entry<K, V>>{
	private final MapStateDescriptor<K, V> descriptor;

	/**
	 * Creates an input format for reading broadcast state from an operator in a savepoint.
	 *
	 * @param savepointPath The path to an existing savepoint.
	 * @param uid The uid of a particular operator.
	 * @param descriptor The descriptor for this state, providing a name and serializer.
	 */
	public BroadcastStateInputFormat(String savepointPath, String uid, MapStateDescriptor<K, V> descriptor) {
		super(savepointPath, uid);
		this.descriptor = descriptor;
	}

	@Override
	protected Iterable<Map.Entry<K, V>> getElements(OperatorStateBackend restoredBackend) throws Exception {
		return restoredBackend.getBroadcastState(descriptor).entries();
	}
}
