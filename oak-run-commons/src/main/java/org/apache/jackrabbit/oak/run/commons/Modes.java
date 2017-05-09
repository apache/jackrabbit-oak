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

package org.apache.jackrabbit.oak.run.commons;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * represent an individual Mode for running a COMMAND. It's a substitution for the old Mode enum we
 * used in order to allow reuse of logic.
 */
public final class Modes {
    private final Map<String, Command> MODES;

    public Modes(Map<String, Command> modes) {
        this.MODES = checkNotNull(modes, "Provided map of Modes cannot be null");
    }

    public Command getCommand(String name) {
        // as the Map already return null in case of not found we don't have to do anything here.
        return MODES.get(name);
    }

    public Iterable<String> getModes() {
        return MODES.keySet();
    }
}
