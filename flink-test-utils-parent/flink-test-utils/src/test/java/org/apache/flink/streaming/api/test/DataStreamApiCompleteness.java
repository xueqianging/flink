package org.apache.flink.streaming.api.test;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DataStreamApiCompleteness {
	private static final Set<String> excludedMethods;

	private static final Set<Pattern> excludedPatterns;

	static {
		excludedMethods  = new HashSet<>();

		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.copy");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.getTransformation");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator.forceNonParallel");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.ConnectedStreams.getExecutionEnvironment");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.ConnectedStreams.getFirstInput");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.ConnectedStreams.getSecondInput");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.ConnectedStreams.getType1");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.ConnectedStreams.getType2");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.ConnectedStreams.addGeneralWindowCombine");

		excludedMethods.add("org.apache.flink.streaming.api.datastream.WindowedStream.getType");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.WindowedStream.getExecutionConfig");

		excludedMethods.add("org.apache.flink.streaming.api.datastream.WindowedStream.getExecutionEnvironment");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.WindowedStream.getInputType");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.AllWindowedStream.getExecutionEnvironment");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.AllWindowedStream.getInputType");

		excludedMethods.add("org.apache.flink.streaming.api.datastream.KeyedStream.getKeySelector");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.KeyedStream.timeWindow");

		excludedMethods.add("org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.isChainingEnabled");
		excludedMethods.add("org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getStateHandleProvider");
		excludedMethods.add("org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getCheckpointInterval");
		excludedMethods.add("org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.addOperator");
		excludedMethods.add("org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getCheckpointingMode");
		excludedMethods.add("org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.isForceCheckpointing");

		// We don't need type hints
		excludedMethods.add("org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator.returns");

		// We don't need reditribution policies
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.broadcast");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.global");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.rebalance");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.split");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.rescale");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.forward");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.shuffle");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.partitionCustom");

		// We explicitly set timestamps so we don't need assigners
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.assignTimestamps");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.assignTimestampsAndWatermarks");

		// We don't need underlying infra
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.getId");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.getParallelism");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.getMinResources");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.getPreferredResources");

		// We don't need to print
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.print");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.printToErr");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.writeAsText");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.writeToSocket");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.writeUsingOutputFormat");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.writeAsCsv");

		// TODO Do We Need These
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.addSink");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.connect");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.iterate");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.coGroup");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.union");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.sum");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.max");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.min");

		//temporary
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.windowAll");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.countWindowAll");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.timeWindowAll");

		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.project");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.getExecutionEnvironment");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.getExecutionConfig");
		excludedMethods.add("org.apache.flink.streaming.api.datastream.DataStream.join");

		excludedPatterns = new HashSet<>();
	}

	@Test
	public void testApi() throws Exception {
		checkMethods(org.apache.flink.streaming.api.datastream.DataStream.class, DataStream.class);
		checkMethods(org.apache.flink.streaming.api.datastream.KeyedStream.class, KeyedStream.class);
	}


	private void checkMethods(Class<?> apiClass, Class<?> testClass) {
		Set<String> apiMethods = Arrays.stream(apiClass.getMethods())
			.filter(method -> !method.isAccessible())
			.filter(this::isExcludedByName)
			.map(Method::getName)
			.collect(Collectors.toSet());

		Set<String> testMethods = Arrays.stream(testClass.getMethods())
			.filter(method -> !method.isAccessible())
			.filter(this::isExcludedByName)
			.map(Method::getName)
			.collect(Collectors.toSet());

		apiMethods.removeAll(testMethods);

		for (String method : apiMethods) {
			Assert.fail("Method " + method + " from " + apiClass +" is missing from " + testClass);
		}
	}

	private boolean isExcludedByName(Method method) {
		Class<?> clazz = method.getDeclaringClass();
		do {
			String name = clazz.getName() + "." + method.getName();
			if (excludedMethods.contains(name)) {
				return false;
			}

			for (Pattern pattern : excludedPatterns) {
				if (pattern.matcher(name).find()) {
					return false;
				}
			}

			clazz = clazz.getSuperclass();
		} while (clazz != null);

		return true;
	}
}
