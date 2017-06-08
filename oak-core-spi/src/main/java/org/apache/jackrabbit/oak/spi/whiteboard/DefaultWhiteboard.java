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

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newIdentityHashSet;
import static java.util.Collections.emptyList;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DefaultWhiteboard implements Whiteboard {

    private final Map<Class<?>, Set<Service>> registry = newHashMap();

    private synchronized <T> void registered(Class<T> type, Service service) {
        Set<Service> services = registry.get(type);
        if (services == null) {
            services = newIdentityHashSet();
            registry.put(type, services);
        }
        services.add(service);
    }

    private synchronized <T> void unregistered(Class<T> type, Service service) {
        Set<Service> services = registry.get(type);
        if (services != null) {
            services.remove(service);
        }
    }

    @SuppressWarnings("unchecked")
    private synchronized <T> List<T> lookup(Class<T> type) {
        Set<Service> services = registry.get(type);
        if (services != null) {
            return (List<T>) services
                    .stream()
                    .map(Service::getService)
                    .collect(Collectors.toList());
        } else {
            return emptyList();
        }
    }

    @SuppressWarnings("unchecked")
    private synchronized <T> List<T> lookup(Class<T> type, Map<String, String> filterProperties) {
        Set<Service> services = registry.get(type);
        if (services != null) {
            return (List<T>) services
                    .stream()
                    .filter(s -> s.matches(filterProperties))
                    .map(Service::getService)
                    .collect(Collectors.toList());
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

        Service s = new Service(service, properties);

        registered(type, s);
        return new Registration() {
            @Override
            public void unregister() {
                unregistered(type, s);
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

    @Override
    public <T> Tracker<T> track(Class<T> type, Map<String, String> filterProperties) {

        checkNotNull(type);
        return new Tracker<T>() {
            @Override
            public List<T> getServices() {
                return lookup(type, filterProperties);
            }
            @Override
            public void stop() {
            }
        };
    }

    private static class Service {

        private final Object service;

        private final Map<?, ?> properties;

        private Service(@Nonnull Object service, Map<?, ?> properties) {
            checkNotNull(service);
            this.service = service;
            this.properties = properties;
        }

        private Object getService() {
            return service;
        }

        private boolean matches(Map<String, String> properties) {
            return properties.entrySet().stream()
                    .allMatch(this::propertyMatches);
        }

        private  boolean propertyMatches(Map.Entry<String, String> filterEntry) {
            String key = filterEntry.getKey();
            String expectedValue = filterEntry.getValue();
            if (properties == null || !properties.containsKey(key)) {
                return expectedValue == null;
            }
            Object value = properties.get(key);
            if (value == null) {
                return expectedValue == null;
            }
            return value.toString().equals(expectedValue);
        }
    }
}
