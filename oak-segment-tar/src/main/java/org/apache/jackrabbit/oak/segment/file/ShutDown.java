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

package org.apache.jackrabbit.oak.segment.file;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An instance of this class encapsulates the shutdown logic of the {@link FileStore}.
 * <p>
 * A shutdown is initiated by calling {@link #shutDown} and completed at the point
 * where the returned {@link ShutDownCloser} has been {@link ShutDownCloser#close() closed}.
 *
 * Code that needs to protect itself from running after a shutdown has been initiated can
 * use the {@link #keepAlive()}, {@link #tryKeepAlive()} and {@link #isShutDown}:
 *
 * <pre>
     try (ShutDownCloser ignored = shutDown.keepAlive()) {
        // protected code here
     }
 * </pre>
 */
class ShutDown {

    /**
     * An {@link AutoCloseable} whose {@link #close()} doesn't throw an exception.
     */
    interface ShutDownCloser extends AutoCloseable {
        @Override
        void close();
    }

    private volatile boolean isShutDown;

    private boolean shutDown;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Keep the store from being shut down until the returned {@link ShutDownCloser}
     * is {@link ShutDownCloser#close() closed}.
     * @throws IllegalStateException if the store is already {@link #isShutDown() shut down}.
     */
    ShutDownCloser keepAlive() {
        lock.readLock().lock();

        if (shutDown) {
            lock.readLock().unlock();
            throw new IllegalStateException("already shut down");
        }

        return () -> lock.readLock().unlock();
    }

    /**
     * Try to keep the store from being shut down until the returned {@link ShutDownCloser}
     * is {@link ShutDownCloser#close() closed}. Callers of this method need to call
     * {@link #isShutDown()} before proceeding.
     */
    ShutDownCloser tryKeepAlive() {
        lock.readLock().lock();
        return () -> lock.readLock().unlock();
    }

    /**
     * Initiate a shutdown of the store. The shutdown is complete once the returned
     * {@link ShutDownCloser} is {@link ShutDownCloser#close() closed}.
     * @return
     */
    ShutDownCloser shutDown() {
        isShutDown = true;
        lock.writeLock().lock();

        if (shutDown) {
            lock.writeLock().unlock();
            throw new IllegalStateException("already shut down");
        }

        return () -> {
            shutDown = true;
            lock.writeLock().unlock();
        };
    }

    /**
     * @return  {@code true} iff the store is shut down.
     */
    boolean isShutDown() {
        return isShutDown;
    }

}
