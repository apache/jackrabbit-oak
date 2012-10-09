package org.apache.jackrabbit.oak.plugins.commit;

import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandlerProvider;

public class AnnotatingConflictHandlerProvider implements ConflictHandlerProvider {
    @Override
    public ConflictHandler getConflictHandler() {
        return new AnnotatingConflictHandler();
    }
}
