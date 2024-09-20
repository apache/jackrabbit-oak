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

public class WriteAccessController {
    private volatile boolean isWritingAllowed = false;

    private Object lock = new Object();

    public void disableWriting() {
        this.isWritingAllowed = false;
    }

    public void enableWriting() {
        this.isWritingAllowed = true;

        synchronized (lock) {
            lock.notifyAll();
        }
    }

    public void checkWritingAllowed() {
        while (!isWritingAllowed) {
            synchronized (lock) {
                if (isWritingAllowed) {
                    return;
                }
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for writing to be allowed", e);
                }
            }
        }
    }
}
