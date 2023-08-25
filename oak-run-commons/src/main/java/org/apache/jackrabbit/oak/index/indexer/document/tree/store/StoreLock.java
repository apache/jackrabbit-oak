/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.index.indexer.document.tree.store;

import java.io.Closeable;
import java.util.Collections;

public class StoreLock implements Closeable {

    private static final String LOCK_NAME = "lock";
    private boolean closed;
    private final Store store;
    private final PageFile lock;

    private StoreLock(Store store, PageFile lock) {
        this.store = store;
        this.lock = lock;
    }

    /**
     * Lock a store.
     *
     * @param store the store
     * @return the store lock
     * @throws IllegalStateException if already locked
     */
    public static StoreLock lock(Store store) throws IllegalStateException {
        PageFile lock = new PageFile(false);
        lock.setFileName(LOCK_NAME);
        lock.appendRecord(LOCK_NAME, null);
        if (!store.putIfAbsent(LOCK_NAME, lock)) {
            throw new IllegalStateException("Already locked");
        }
        return new StoreLock(store, lock);
    }

    /**
     * Set the lock token.
     *
     * @param token the token
     */
    public void setLockToken(String token) {
        if (closed) {
            throw new IllegalStateException("Already closed");
        }
        lock.setValue(0, token);
        store.put(LOCK_NAME, lock);
    }

    /**
     * Retrieve the lock token.
     *
     * @param store the store
     * @return the token
     */
    public static String getLockToken(Store store) {
        PageFile lock = store.getIfExists(LOCK_NAME);
        if (lock == null) {
            return null;
        }
        return lock.getValue(0);
    }

    /**
     * Force-unlock.
     *
     * @param store the store
     */
    public static void forceUnlock(Store store) {
        store.remove(Collections.singleton(LOCK_NAME));
    }

    /**
     * Unlock the store.
     */
    public void close() {
        closed = true;
        store.remove(Collections.singleton(LOCK_NAME));
    }

}
