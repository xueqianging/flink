package org.apache.flink.connectors.savepoint.apiv2;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * Returns the max parallelism for a subtask.
 *
 * <b>IMPORTANT:</b> This interface is structured as
 * a supplier so that max parallelism's calculated off existing
 * savepoints are generated cluster side with all filesystem's
 * on the classpath.
 */
@FunctionalInterface
public interface MaxParallelismSupplier extends Supplier<Integer>, Serializable {

	/**
	 * A supplier for known max parallelism.
	 */
	static MaxParallelismSupplier of(int maxParallelism) {
		return (MaxParallelismSupplier) () -> maxParallelism;
	}
}
