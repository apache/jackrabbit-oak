package org.apache.jackrabbit.oak.plugins.commit;

import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandlerProvider;

public class AnnotatingConflictHandlerProvider implements ConflictHandlerProvider {
    @Override
    public ConflictHandler getConflictHandler(CoreValueFactory valueFactory) {
        return new AnnotatingConflictHandler(valueFactory);
    }
}
