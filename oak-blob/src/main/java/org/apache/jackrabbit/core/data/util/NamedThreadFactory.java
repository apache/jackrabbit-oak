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

package org.apache.jackrabbit.core.data.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class extends {@link ThreadFactory} to creates named threads.
 */
public class NamedThreadFactory implements ThreadFactory {

    private AtomicInteger threadCount = new AtomicInteger(1);

    String threadPrefixName;

    public NamedThreadFactory(String threadPrefixName) {
        super();
        this.threadPrefixName = threadPrefixName;
    }

    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setContextClassLoader(getClass().getClassLoader());
        thread.setName(threadPrefixName + "-" + threadCount.getAndIncrement());
        return thread;
    }

}
