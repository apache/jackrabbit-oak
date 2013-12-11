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

package org.apache.jackrabbit.oak.osgi;

import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.Executor;

import org.osgi.framework.BundleContext;
import org.osgi.util.tracker.ServiceTracker;

/**
 * OSGi implementation of an {@link OakExecutor} looking it
 * up from the service registry.
 */
public class OsgiExecutor implements OakExecutor {
    private ServiceTracker executorTracker;

    @Override
    public void execute(Runnable command) {
        try {
            getExecutor().execute(command);
        } catch (InterruptedException e) {
            // Not much we can do here. Let's hope someone up
            // the stack sees the interrupted flag.
            Thread.currentThread().interrupt();
        }
    }

    private Executor getExecutor() throws InterruptedException {
        return (Executor) executorTracker.waitForService(0);
    }

    public void start(BundleContext bundleContext) {
        checkState(executorTracker == null);
        executorTracker = new ServiceTracker(bundleContext, OakExecutor.class.getName(), null);
        executorTracker.open();
    }

    public void stop() {
        checkState(executorTracker != null);
        executorTracker.close();
    }
}
