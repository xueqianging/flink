package org.apache.flink.connectors.savepoint.input;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.connectors.savepoint.functions.KeyedStateReaderFunction;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.BiConsumerWithException;

import java.util.HashSet;
import java.util.Set;

@SuppressWarnings("unchecked")
class ProcessReaderContext implements KeyedStateReaderFunction.Context {
	private final ListState<Long> eventTimers;

	private final ListState<Long> procTimers;

	private final Set<Long> eventTimerSet;

	private final Set<Long> procTimerSet;

	static ProcessReaderContext create(
		String timerServiceName,
		InternalTimerService<VoidNamespace> timerService,
		AbstractKeyedStateBackend<?> keyedStateBackend
	) throws Exception {

		ListStateDescriptor<Long> eventTimerDescriptor = new ListStateDescriptor<>("event-timers", Types.LONG);
		ListStateDescriptor<Long> procTimerDescriptor = new ListStateDescriptor<>("proc-timers", Types.LONG);

		ListState<Long> eventTimers = keyedStateBackend.getPartitionedState(
			timerServiceName,
			StringSerializer.INSTANCE,
			eventTimerDescriptor);

		ListState<Long> procTimers = keyedStateBackend.getPartitionedState(
			timerServiceName,
			StringSerializer.INSTANCE,
			procTimerDescriptor);

		timerService.forEachEventTimeTimer(copyTimers(eventTimers));

		timerService.forEachProcessingTimeTimer(copyTimers(procTimers));

		return new ProcessReaderContext(eventTimers, procTimers);
	}

	private static <N> BiConsumerWithException<N, Long, Exception> copyTimers(ListState<Long> timers) {
		return (namespace, timestamp) -> {
			if (namespace.equals(VoidNamespace.INSTANCE)) {
				timers.add(timestamp);
			}
		};
	}

	private ProcessReaderContext(ListState<Long> eventTimers, ListState<Long> procTimers) {
		this.eventTimers = eventTimers;
		this.procTimers = procTimers;

		this.eventTimerSet = new HashSet<>();
		this.procTimerSet = new HashSet<>();
	}

	@Override
	public Set<Long> getEventTimeTimers() {
		try {
			eventTimerSet.clear();
			eventTimers.get().forEach(eventTimerSet::add);

			return eventTimerSet;
		} catch (Exception e) {
			throw new FlinkRuntimeException(e);
		}
	}

	@Override
	public Set<Long> getProcessingTimeTimers() {
		try {
			procTimerSet.clear();
			procTimers.get().forEach(procTimerSet::add);

			return procTimerSet;
		} catch (Exception e) {
			throw new FlinkRuntimeException(e);
		}
	}
}
