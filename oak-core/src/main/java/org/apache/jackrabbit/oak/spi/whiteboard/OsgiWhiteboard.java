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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Dictionary;

import javax.annotation.Nonnull;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

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
            Class<T> type, T service, Dictionary<?, ?> properties) {
        final ServiceRegistration registration =
                context.registerService(type.getName(), service, properties);
        return new Registration() {
            @Override
            public void unregister() {
                registration.unregister();
            }
        };
    }

}
