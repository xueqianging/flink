package org.apache.flink.state.api.input;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.input.splits.KeyGroupRangeInputSplit;
import org.apache.flink.state.api.runtime.NeverFireProcessingTimeService;
import org.apache.flink.state.api.runtime.SavepointEnvironment;
import org.apache.flink.state.api.runtime.SavepointRuntimeContext;
import org.apache.flink.state.api.runtime.VoidTriggerable;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.StreamOperatorStateContext;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * The base input format for reading keyed state.
 *
 * @param <IT> The type that is iterated on in the state backend.
 * @param <K> The key type.
 * @param <N> The namespace type.
 * @param <OUT> The output type.
 * @param <F> The user function type.
 */
@Internal
@SuppressWarnings("WeakerAccess")
abstract class KeyedStateBaseInputFormat<IT, K, N, OUT, F extends Function> extends RichInputFormat<OUT, KeyGroupRangeInputSplit> implements KeyContext {

	private final OperatorState operatorState;

	private final StateBackend stateBackend;

	private final TypeInformation<K> keyType;

	private final String timersName;

	protected final F userFunction;

	protected final TypeSerializer<N> namespaceSerializer;

	private transient TypeSerializer<K> keySerializer;

	protected transient BufferingCollector<OUT> out;

	private transient CloseableRegistry registry;

	protected transient Iterator<IT> keys;

	protected transient AbstractKeyedStateBackend<K> keyedStateBackend;

	protected transient InternalTimerService<N> timerService;

	KeyedStateBaseInputFormat(OperatorState operatorState, StateBackend stateBackend, TypeInformation<K> keyType, String timersName, F userFunction, TypeSerializer<N> namespaceSerializer) {
		Preconditions.checkNotNull(operatorState, "The operator state cannot be null");
		Preconditions.checkNotNull(stateBackend, "The state backend cannot be null");
		Preconditions.checkNotNull(keyType, "The key type information cannot be null");
		Preconditions.checkNotNull(timersName, "The timers name cannot be null");
		Preconditions.checkNotNull(userFunction, "The userfunction cannot be null");
		Preconditions.checkNotNull(namespaceSerializer, "The namespace serializer cannot be null");

		this.operatorState = operatorState;
		this.stateBackend = stateBackend;
		this.keyType = keyType;
		this.timersName = timersName;
		this.userFunction = userFunction;
		this.namespaceSerializer = namespaceSerializer;
	}

	abstract Iterator<IT> open(SavepointRuntimeContext ctx) throws Exception;

	abstract void readKey(IT key, Collector<OUT> out) throws Exception;

	abstract K getUserKey(IT record);

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(KeyGroupRangeInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return cachedStatistics;
	}

	@Override
	public KeyGroupRangeInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		final int maxParallelism = operatorState.getMaxParallelism();

		final List<KeyGroupRange> keyGroups = sortedKeyGroupRanges(minNumSplits, maxParallelism);

		return CollectionUtil.mapWithIndex(
			keyGroups,
			(keyGroupRange, index) -> createKeyGroupRangeInputSplit(
				operatorState,
				maxParallelism,
				keyGroupRange,
				index)
		).toArray(KeyGroupRangeInputSplit[]::new);
	}

	private static KeyGroupRangeInputSplit createKeyGroupRangeInputSplit(
		OperatorState operatorState,
		int maxParallelism,
		KeyGroupRange keyGroupRange,
		Integer index) {

		final List<KeyedStateHandle> managedKeyedState = StateAssignmentOperation.getManagedKeyedStateHandles(operatorState, keyGroupRange);
		final List<KeyedStateHandle> rawKeyedState = StateAssignmentOperation.getRawKeyedStateHandles(operatorState, keyGroupRange);

		return new KeyGroupRangeInputSplit(managedKeyedState, rawKeyedState, maxParallelism, index);
	}

	@Nonnull
	private static List<KeyGroupRange> sortedKeyGroupRanges(int minNumSplits, int maxParallelism) {
		List<KeyGroupRange> keyGroups = StateAssignmentOperation.createKeyGroupPartitions(
			maxParallelism,
			Math.min(minNumSplits, maxParallelism));

		keyGroups.sort(Comparator.comparing(KeyGroupRange::getStartKeyGroup));
		return keyGroups;
	}

	@Override
	public void openInputFormat() {
		out = new BufferingCollector<>();
		keySerializer = keyType.createSerializer(getRuntimeContext().getExecutionConfig());
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open(KeyGroupRangeInputSplit split) throws IOException {
		registry = new CloseableRegistry();

		final Environment environment = new SavepointEnvironment
			.Builder(getRuntimeContext(), split.getNumKeyGroups())
			.setSubtaskIndex(split.getSplitNumber())
			.setPrioritizedOperatorSubtaskState(split.getPrioritizedOperatorSubtaskState())
			.build();

		final StreamOperatorStateContext context = getStreamOperatorStateContext(environment);

		keyedStateBackend = (AbstractKeyedStateBackend<K>) context.keyedStateBackend();

		final DefaultKeyedStateStore keyedStateStore = new DefaultKeyedStateStore(keyedStateBackend, getRuntimeContext().getExecutionConfig());
		SavepointRuntimeContext ctx = new SavepointRuntimeContext(getRuntimeContext(), keyedStateStore);
		timerService = restoreTimerService(context);

		FunctionUtils.setFunctionRuntimeContext(userFunction, ctx);

		try  {
			FunctionUtils.openFunction(userFunction, new Configuration());
			ctx.disableStateRegistration();
			keys = open(ctx);
		} catch (Exception e) {
			throw new IOException("Failed to open user defined function", e);
		}
	}

	private StreamOperatorStateContext getStreamOperatorStateContext(Environment environment) throws IOException {
		StreamTaskStateInitializer initializer = new StreamTaskStateInitializerImpl(
			environment,
			stateBackend,
			new NeverFireProcessingTimeService());

		try {
			return initializer.streamOperatorStateContext(
				operatorState.getOperatorID(),
				operatorState.getOperatorID().toString(),
				this,
				keySerializer,
				registry,
				getRuntimeContext().getMetricGroup());
		} catch (Exception e) {
			throw new IOException("Failed to restore state backend", e);
		}
	}

	@Override
	public void close() throws IOException {
		registry.close();
	}

	@Override
	public boolean reachedEnd() {
		return !out.hasNext() && !keys.hasNext();
	}

	@Override
	public OUT nextRecord(OUT reuse) throws IOException {
		if (out.hasNext()) {
			return out.next();
		}

		final IT key = keys.next();
		setCurrentKey(getUserKey(key));

		try {
			readKey(key, out);
		} catch (Exception e) {
			throw new IOException("User defined function threw an exception", e);
		}

		keys.remove();

		return out.next();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void setCurrentKey(Object key) {
		keyedStateBackend.setCurrentKey((K) key);
	}

	@Override
	public Object getCurrentKey() {
		return keyedStateBackend.getCurrentKey();
	}

	@SuppressWarnings("unchecked")
	private InternalTimerService<N> restoreTimerService(StreamOperatorStateContext context) {
		InternalTimeServiceManager<K> timeServiceManager = (InternalTimeServiceManager<K>) context.internalTimerServiceManager();
		TimerSerializer<K, N> timerSerializer = new TimerSerializer<>(keySerializer, namespaceSerializer);
		return timeServiceManager.getInternalTimerService(timersName, timerSerializer, VoidTriggerable.instance());
	}
}
