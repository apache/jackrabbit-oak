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
package org.apache.jackrabbit.oak.segment.remote;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WriteAccessController {
    private boolean isWritingAllowed = false;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public void disableWriting() {
        try {


        lock.writeLock().lock();
        try {
            this.isWritingAllowed = false;
        } finally {
            lock.writeLock().unlock();
        }

        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public void enableWriting() {
        try {
            lock.writeLock().lock();
            try {
                this.isWritingAllowed = true;
                synchronized (this) {
                    this.notifyAll();
                }
            } finally {
                lock.writeLock().unlock();
            }

        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public void checkWritingAllowed() {
        lock.readLock().lock();
        try {
            while (!isWritingAllowed) {
                synchronized (this) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while waiting for writing to be allowed", e);
                    }
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }
}
