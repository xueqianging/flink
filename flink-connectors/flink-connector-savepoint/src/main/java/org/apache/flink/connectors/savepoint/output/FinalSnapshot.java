package org.apache.flink.connectors.savepoint.output;

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;

/**
 * Takes a final snapshot of the state of an operator subtask.
 */
final class FinalSnapshot {
	static <OUT, OP extends StreamOperator<OUT>> OperatorSubtaskState snapshot(
		CheckpointStorageWorkerView checkpointStorage,
		OP operator,
		CheckpointMetaData metaData,
		CheckpointOptions options) throws Exception {

		operator.prepareSnapshotPreBarrier(0);

		CheckpointStreamFactory storage = checkpointStorage.resolveCheckpointStorageLocation(
			metaData.getCheckpointId(),
			options.getTargetLocation());

		operator.prepareSnapshotPreBarrier(0L);

		OperatorSnapshotFutures snapshotInProgress = operator.snapshotState(
			metaData.getCheckpointId(),
			metaData.getTimestamp(),
			options,
			storage);

		OperatorSubtaskState state = new OperatorSnapshotFinalizer(snapshotInProgress).getJobManagerOwnedState();

		operator.notifyCheckpointComplete(0);
		return state;
	}
}
