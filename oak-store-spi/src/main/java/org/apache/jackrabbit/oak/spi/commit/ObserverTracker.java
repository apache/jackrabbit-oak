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

package org.apache.jackrabbit.oak.spi.commit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;

import java.io.Closeable;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

public class ObserverTracker implements ServiceTrackerCustomizer {
    private final Map<ServiceReference, Closeable> subscriptions = newHashMap();
    private final Observable observable;

    private BundleContext bundleContext;
    private ServiceTracker observerTracker;

    public ObserverTracker(@Nonnull Observable observable) {
        this.observable = checkNotNull(observable);
    }

    public void start(@Nonnull BundleContext bundleContext) {
        checkState(this.bundleContext == null);
        this.bundleContext = checkNotNull(bundleContext);
        observerTracker = new ServiceTracker(bundleContext, Observer.class.getName(), this);
        observerTracker.open();
    }

    public void stop() {
        checkState(this.bundleContext != null);
        observerTracker.close();
    }

    //------------------------< ServiceTrackerCustomizer >----------------------

    @Override
    public Object addingService(ServiceReference reference) {
        Object service = bundleContext.getService(reference);
        if (service instanceof Observer) {
            subscriptions.put(reference, observable.addObserver((Observer) service));
            return service;
        } else {
            bundleContext.ungetService(reference);
            return null;
        }
    }

    @Override
    public void modifiedService(ServiceReference reference, Object service) {
        // nothing to do
    }

    @Override
    public void removedService(ServiceReference reference, Object service) {
        Closeable subscription = subscriptions.remove(reference);
        if (subscription != null) {
            closeQuietly(subscription);
            bundleContext.ungetService(reference);
        }
    }

}
