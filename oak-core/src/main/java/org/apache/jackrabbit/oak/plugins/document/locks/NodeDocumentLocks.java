package org.apache.jackrabbit.oak.plugins.document.locks;

import java.util.concurrent.locks.Lock;

public interface NodeDocumentLocks {

    /**
     * Acquires a log for the given key.
     *
     * @param key a key.
     * @return the acquired lock for the given key.
     */
    Lock acquire(String key);

}
