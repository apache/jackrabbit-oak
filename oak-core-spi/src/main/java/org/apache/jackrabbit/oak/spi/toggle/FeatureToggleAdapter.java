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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;

/**
 * A toggle adapter is registered with the {@link Whiteboard} and can be
 * discovered by third party code to control the state of feature toggles.
 */
public final class FeatureToggleAdapter {

    private final String name;

    private final AtomicBoolean state;

    /**
     * Create a new adapter with a given name and value.
     *
     * @param name the name of the feature toggle.
     * @param state the state for the feature toggle.
     */
    public FeatureToggleAdapter(String name, AtomicBoolean state) {
        this.name = name;
        this.state = state;
    }

    /**
     * @return the name of the feature toggle.
     */
    public String getName() {
        return name;
    }

    /**
     * Changes the state of the feature toggle.
     *
     * @param state the new state of the feature toggle.
     */
    public void setEnabled(boolean state) {
        this.state.set(state);
    }
}
