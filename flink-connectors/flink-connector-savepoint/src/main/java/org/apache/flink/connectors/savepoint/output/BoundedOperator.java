package org.apache.flink.connectors.savepoint.output;

import org.apache.flink.annotation.Internal;

/**
 * A shim interface until proper bounded inputs
 * are supported by the stream tasks.
 */
@Internal
public interface BoundedOperator {
	void endInput() throws Exception;
}
