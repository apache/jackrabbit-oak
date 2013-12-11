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
package org.apache.jackrabbit.oak.spi.whiteboard;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newIdentityHashSet;
import static java.util.Collections.emptyList;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultWhiteboard implements Whiteboard {

    private final Map<Class<?>, Set<Object>> registry = newHashMap();

    private synchronized <T> void registered(Class<T> type, T service) {
        Set<Object> services = registry.get(type);
        if (services == null) {
            services = newIdentityHashSet();
            registry.put(type, services);
        }
        services.add(service);
    }

    private synchronized <T> void unregistered(Class<T> type, T service) {
        Set<Object> services = registry.get(type);
        if (services != null) {
            services.remove(service);
        }
    }

    @SuppressWarnings("unchecked")
    private synchronized <T> List<T> lookup(Class<T> type) {
        Set<Object> services = registry.get(type);
        if (services != null) {
            return (List<T>) newArrayList(services);
        } else {
            return emptyList();
        }
    }

    //--------------------------------------------------------< Whiteboard >--

    @Override
    public <T> Registration register(
            final Class<T> type, final T service, Map<?, ?> properties) {
        checkNotNull(type);
        checkNotNull(service);
        checkArgument(type.isInstance(service));
        registered(type, service);
        return new Registration() {
            @Override
            public void unregister() {
                unregistered(type, service);
            }
        };
    }

    @Override
    public <T> Tracker<T> track(final Class<T> type) {
        checkNotNull(type);
        return new Tracker<T>() {
            @Override
            public List<T> getServices() {
                return lookup(type);
            }
            @Override
            public void stop() {
            }
        };
    }

}
