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

package org.apache.jackrabbit.oak.run.osgi;

import java.io.Closeable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleEvent;
import org.osgi.framework.Filter;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.SynchronousBundleListener;
import org.osgi.util.tracker.ServiceTracker;

public class RunnableJobTracker extends ServiceTracker<Runnable, Future>
        implements Closeable, SynchronousBundleListener {
    /**
     * Lazily loaded executor
     */
    private final Supplier<ScheduledExecutorService> executor =
            Suppliers.memoize(new Supplier<ScheduledExecutorService>() {
        @Override
        public ScheduledExecutorService get() {
            return Oak.defaultScheduledExecutor();
        }
    });

    public RunnableJobTracker(BundleContext context) {
        super(context, createFilter(), null);
        open();
        context.addBundleListener(this);
    }

    @Override
    public Future addingService(ServiceReference<Runnable> reference) {
        Runnable runnable = context.getService(reference);
        long period = PropertiesUtil.toLong(reference.getProperty("scheduler.period"), -1);
        boolean concurrent = PropertiesUtil.toBoolean(reference.getProperty("scheduler.concurrent"), false);
        Future future = null;
        if (period != -1) {
            if (concurrent) {
                future = getExecutor().scheduleAtFixedRate(
                        runnable, period, period, TimeUnit.SECONDS);
            } else {
                future = getExecutor().scheduleWithFixedDelay(
                        runnable, period, period, TimeUnit.SECONDS);
            }
        }
        return future;
    }

    @Override
    public void removedService(ServiceReference reference, Future future) {
        future.cancel(false);
    }

    @Override
    public void close() {
        super.close();
        getExecutor().shutdown();
    }

    @Override
    public void bundleChanged(BundleEvent bundleEvent) {
        //Look for close event of system bundle to shutdown executor
        //Ideally we should listen to FrameworkEvent but PojoSR
        //currently does not emit framework event
        if(bundleEvent.getBundle().getBundleId() == 0
                && bundleEvent.getType() == bundleEvent.STOPPED){
            close();
        }
    }

    private ScheduledExecutorService getExecutor(){
        return executor.get();
    }

    private static Filter createFilter()  {
        try {
            return FrameworkUtil.createFilter("(&(objectclass=java.lang.Runnable)(scheduler.period=*))");
        } catch (InvalidSyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
