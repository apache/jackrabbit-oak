package org.apache.jackrabbit.oak.spi.commit;

public interface ConflictHandlerProvider {
    ConflictHandler getConflictHandler();
}
