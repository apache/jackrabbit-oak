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

import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;

/**
 * A feature toggle to control new functionality. The default state of a feature
 * toggle is {@code false} and can be controlled by third party code discovering
 * {@link FeatureToggleAdapter}s on the {@link Whiteboard}. Every adapter is
 * linked to a feature toggle and allows to control the state of the feature
 * toggle via {@link FeatureToggleAdapter#setEnabled(boolean)}.
 */
public final class FeatureToggle implements Closeable {

    private final AtomicBoolean value;

    private final Registration registration;

    private FeatureToggle(AtomicBoolean value, Registration registration) {
        this.value = value;
        this.registration = registration;
    }

    /**
     * Creates a new {@link FeatureToggle} with the given name and registers the
     * corresponding {@link FeatureToggleAdapter} on the {@link Whiteboard}.
     * Client code must call {@link FeatureToggle#close()} when the toggle is
     * not used anymore.
     *
     * @param name the name of the feature toggle.
     * @param whiteboard the whiteboard where to register the toggle adapter.
     * @return the feature toggle.
     */
    public static FeatureToggle newFeatureToggle(String name, Whiteboard whiteboard) {
        AtomicBoolean value = new AtomicBoolean();
        FeatureToggleAdapter adapter = new FeatureToggleAdapter(name, value);
        return new FeatureToggle(value, whiteboard.register(
                FeatureToggleAdapter.class, adapter, Collections.emptyMap()));
    }

    /**
     * @return the current state of this toggle.
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
