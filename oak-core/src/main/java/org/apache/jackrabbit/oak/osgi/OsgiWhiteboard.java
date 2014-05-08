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
import static java.util.Arrays.asList;

import java.util.Collections;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;

/**
 * OSGi-based whiteboard implementation.
 */
public class OsgiWhiteboard implements Whiteboard {

    private final BundleContext context;

    public OsgiWhiteboard(@Nonnull BundleContext context) {
        this.context = checkNotNull(context);
    }

    @Override
    public <T> Registration register(
            Class<T> type, T service, Map<?, ?> properties) {
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
            @Override
            public void unregister() {
                registration.unregister();
            }
        };
    }

    @Override
    public <T> Tracker<T> track(Class<T> type) {
        checkNotNull(type);
        final ServiceTracker tracker =
                new ServiceTracker(context, type.getName(), null);
        tracker.open();
        return new Tracker<T>() {
            @Override @SuppressWarnings("unchecked")
            public List<T> getServices() {
                Object[] services = tracker.getServices();
                return (List<T>) (services != null ? asList(services) : Collections.emptyList());
            }
            @Override
            public void stop() {
                tracker.close();
            }
        };
    }

}
