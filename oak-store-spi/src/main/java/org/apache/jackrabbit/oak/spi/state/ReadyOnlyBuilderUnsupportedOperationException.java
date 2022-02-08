package org.apache.jackrabbit.oak.spi.state;

/**
 * Indicates that a modification operation was tried to execute on a read-only builder.
 * It should be used instead of throwing plain UnsupportedOperationExceptions in that situation.
 */
public class ReadyOnlyBuilderUnsupportedOperationException extends UnsupportedOperationException {

    public ReadyOnlyBuilderUnsupportedOperationException (String reason) {
        super(reason);
    }
}
