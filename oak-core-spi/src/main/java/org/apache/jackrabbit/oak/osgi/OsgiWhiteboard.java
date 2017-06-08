/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.osgi;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newTreeMap;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.osgi.OsgiUtil.getFilter;

import java.util.Collections;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Filter;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi-based whiteboard implementation.
 */
public class OsgiWhiteboard implements Whiteboard {

    private static final Logger log = LoggerFactory
            .getLogger(OsgiWhiteboard.class);

    private final BundleContext context;

    public OsgiWhiteboard(@Nonnull BundleContext context) {
        this.context = checkNotNull(context);
    }

    @Override
    public <T> Registration register(
            final Class<T> type, final T service, Map<?, ?> properties) {
        checkNotNull(type);
        checkNotNull(service);
        checkNotNull(properties);
        checkArgument(type.isInstance(service));

        Dictionary<Object, Object> dictionary = new Hashtable<Object, Object>();
        for (Map.Entry<?, ?> entry : properties.entrySet()) {
            dictionary.put(entry.getKey(), entry.getValue());
        }

        final ServiceRegistration registration =
                context.registerService(type.getName(), service, dictionary);
        return new Registration() {
            private volatile boolean unregistered;
            @Override
            public void unregister() {
                try {
                    if (!unregistered) {
                        registration.unregister();
                        unregistered = true;
                    } else {
                        log.warn("Service {} of type {} unregistered multiple times", service, type);
                    }
                } catch (IllegalStateException ex) {
                    log.warn("Error unregistering service: {} of type {}",
                            service, type.getName(), ex);
                }
            }
        };
    }

    /**
     * Returns a tracker for services of the given type. The returned tracker
     * is optimized for frequent {@link Tracker#getServices()} calls through
     * the use of a pre-compiled list of services that's atomically updated
     * whenever services are added, modified or removed.
     */
    @Override
    public <T> Tracker<T> track(final Class<T> type) {
        return track(type, emptyMap());
    }

    @Override
    public <T> Tracker<T> track(Class<T> type, Map<String, String> filterProperties) {
        return track(type, getFilter(type, filterProperties));
    }

    private <T> Tracker<T> track(Class<T> type, Filter filter) {
        checkNotNull(type);
        final AtomicReference<List<T>> list =
                new AtomicReference<List<T>>(Collections.<T>emptyList());
        final ServiceTrackerCustomizer customizer =
                new ServiceTrackerCustomizer() {
                    private final Map<ServiceReference, T> services =
                            newHashMap();
                    @Override @SuppressWarnings("unchecked")
                    public synchronized Object addingService(
                            ServiceReference reference) {
                        Object service = context.getService(reference);
                        if (type.isInstance(service)) {
                            services.put(reference, (T) service);
                            list.set(getServiceList(services));
                            return service;
                        } else {
                            context.ungetService(reference);
                            return null;
                        }
                    }
                    @Override @SuppressWarnings("unchecked")
                    public synchronized void modifiedService(
                            ServiceReference reference, Object service) {
                        // TODO: Figure out if the old reference instance
                        // would automatically reflect the updated properties.
                        // For now we play it safe by replacing the old key
                        // with the new reference instance passed as argument.
                        services.remove(reference);
                        services.put(reference, (T) service);
                        list.set(getServiceList(services));
                    }
                    @Override
                    public synchronized void removedService(
                            ServiceReference reference, Object service) {
                        services.remove(reference);
                        list.set(getServiceList(services));
                        // TODO: Note that the service might still be in use
                        // by some client that called getServices() before
                        // this method was invoked.
                        context.ungetService(reference);
                    }
                };

        final ServiceTracker tracker = new ServiceTracker(context, filter, customizer);
        tracker.open();
        return new Tracker<T>() {
            @Override
            public List<T> getServices() {
                return list.get();
            }
            @Override
            public void stop() {
                tracker.close();
            }
        };
    }

    /**
     * Utility method that sorts the service objects in the given map
     * according to their service rankings and returns the resulting list.
     *
     * @param services currently available services
     * @return ordered list of the services
     */
    private static <T> List<T> getServiceList(
            Map<ServiceReference, T> services) {
        switch (services.size()) {
        case 0:
            return emptyList();
        case 1:
            return singletonList(
                    services.values().iterator().next());
        default:
            SortedMap<ServiceReference, T> sorted = newTreeMap();
            sorted.putAll(services);
            return newArrayList(sorted.values());
        }
    }

}
