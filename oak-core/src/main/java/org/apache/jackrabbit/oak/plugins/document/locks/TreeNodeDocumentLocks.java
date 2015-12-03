/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.locks;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;

import com.google.common.util.concurrent.Striped;

public class TreeNodeDocumentLocks implements NodeDocumentLocks {

    /**
     * Locks to ensure cache consistency on reads, writes and invalidation.
     */
    private final Striped<Lock> locks = Striped.lock(4096);

    /**
     * ReadWriteLocks to synchronize cache access when child documents are
     * requested from MongoDB and put into the cache. Accessing a single
     * document in the cache will acquire a read (shared) lock for the parent
     * key in addition to the lock (from {@link #locks}) for the individual
     * document. Reading multiple sibling documents will acquire a write
     * (exclusive) lock for the parent key. See OAK-1897.
     */
    private final Striped<ReadWriteLock> parentLocks = Striped.readWriteLock(2048);

    /**
     * Counts how many times {@link TreeLock}s were acquired.
     */
    private volatile AtomicLong lockAcquisitionCounter;

    /**
     * Acquires a log for the given key. The returned tree lock will also hold
     * a shared lock on the parent key.
     *
     * @param key a key.
     * @return the acquired lock for the given key.
     */
    @Override
    public TreeLock acquire(String key) {
        if (lockAcquisitionCounter != null) {
            lockAcquisitionCounter.incrementAndGet();
        }
        TreeLock lock = TreeLock.shared(parentLocks.get(getParentId(key)), locks.get(key));
        lock.lock();
        return lock;
    }

    /**
     * Acquires an exclusive lock on the given parent key. Use this method to
     * block cache access for child keys of the given parent key.
     *
     * @param parentKey the parent key.
     * @return the acquired lock for the given parent key.
     */
    public TreeLock acquireExclusive(String parentKey) {
        if (lockAcquisitionCounter != null) {
            lockAcquisitionCounter.incrementAndGet();
        }
        TreeLock lock = TreeLock.exclusive(parentLocks.get(parentKey));
        lock.lock();
        return lock;
    }

    /**
     * Returns the parent id for the given id. An empty String is returned if
     * the given value is the id of the root document or the id for a long path.
     *
     * @param id an id for a document.
     * @return the id of the parent document or the empty String.
     */
    @Nonnull
    private static String getParentId(@Nonnull String id) {
        String parentId = Utils.getParentId(checkNotNull(id));
        if (parentId == null) {
            parentId = "";
        }
        return parentId;
    }

    public void resetLockAcquisitionCount() {
        lockAcquisitionCounter = new AtomicLong();
    }

    public long getLockAcquisitionCount() {
        if (lockAcquisitionCounter == null) {
            throw new IllegalStateException("The counter hasn't been initialized");
        }
        return lockAcquisitionCounter.get();
    }

    private final static class TreeLock implements Lock {

        private final Lock parentLock;

        private final Lock lock;

        private TreeLock(Lock parentLock, Lock lock) {
            this.parentLock = parentLock;
            this.lock = lock;
        }

        private static TreeLock shared(ReadWriteLock parentLock, Lock lock) {
            return new TreeLock(parentLock.readLock(), lock);
        }

        private static TreeLock exclusive(ReadWriteLock parentLock) {
            return new TreeLock(parentLock.writeLock(), null);
        }

        @Override
        public void lock() {
            parentLock.lock();
            if (lock != null) {
                lock.lock();
            }
        }

        @Override
        public void unlock() {
            if (lock != null) {
                lock.unlock();
            }
            parentLock.unlock();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryLock() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }
    }

}
