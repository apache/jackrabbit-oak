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
package org.apache.jackrabbit.oak.spi.toggle;

import java.io.Closeable;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A feature toggle to control new functionality. The default state of a feature
 * is {@code false} and can be controlled by third party code discovering
 * {@link FeatureToggle}s on the {@link Whiteboard}. Every feature is
 * linked to a feature toggle and allows to control the state of the feature
 * toggle via {@link FeatureToggle#setEnabled(boolean)}. Creating a new feature
 * involves registering a feature toggle on the {@link Whiteboard} and
 * potentially comes with some overhead (e.g. when the whiteboard is based on
 * OSGi). Therefore, client code should not create a new feature, check the
 * state and then immediately release/close it again. Instead, a feature should
 * be acquired initially, checked at runtime whenever needed and finally
 * released when the client component is destroyed.
 * <p>
 * The default state of {@code false} can be overridden by a system property
 * using {@linkplain #newFeatureWithSystemPropertyDefault(String, Whiteboard)},
 * in which case The property name is derived from the toggle name. This helps to quickly verify
 * a new feature. For a toggle named {@code "foo"}, the system property name is
 * {@code oak-feature.foo}.
 */
public final class Feature implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(Feature.class);

    private final AtomicBoolean value;

    private final Registration registration;

    private Feature(AtomicBoolean value, Registration registration) {
        this.value = value;
        this.registration = registration;
    }

    private static Feature internalNewFeatureWithSystemPropertyDefault(String name, Whiteboard whiteboard, boolean withSysPropDefault) {
        // by default the initial value is false, but it can be overridden by a system property
        AtomicBoolean value = withSysPropDefault ? new AtomicBoolean(
                SystemPropertySupplier.create("oak-feature." + name, false).
                        loggingTo(LOG).get()) : new AtomicBoolean();
        FeatureToggle adapter = new FeatureToggle(name, value);
        return new Feature(value, whiteboard.register(
                FeatureToggle.class, adapter, Collections.emptyMap()));
    }

    /**
     * Creates a new {@link Feature} with the given name and registers the
     * corresponding {@link FeatureToggle} on the {@link Whiteboard}.
     * Client code must call {@link Feature#close()} when the toggle is
     * not used anymore.
     *
     * @param name the name of the feature toggle.
     * @param whiteboard the whiteboard where to register the feature toggle.
     * @return the feature toggle.
     */
    public static Feature newFeature(String name, Whiteboard whiteboard) {
        return internalNewFeatureWithSystemPropertyDefault(name, whiteboard, false);
    }

    /**
     * Same as {@linkplain #newFeature(String, Whiteboard)}, but with the initial state provided
     * by a system property, named based on the feature's name.
     */
    public static Feature newFeatureWithSystemPropertyDefault(String name, Whiteboard whiteboard) {
        return internalNewFeatureWithSystemPropertyDefault(name, whiteboard, true);
    }

    /**
     * @return the current state of this feature.
     */
    public boolean isEnabled() {
        return value.get();
    }

    /**
     * Releases resources for this feature toggle.
     */
    @Override
    public void close() {
        this.registration.unregister();
    }
}
