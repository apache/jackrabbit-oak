package org.apache.jackrabbit.oak.spi.commit;

import org.apache.jackrabbit.oak.api.CoreValueFactory;

public interface ConflictHandlerProvider {
    ConflictHandler getConflictHandler(CoreValueFactory valueFactory);
}
