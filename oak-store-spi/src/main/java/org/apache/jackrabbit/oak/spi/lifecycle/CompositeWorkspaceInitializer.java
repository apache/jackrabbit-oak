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
package org.apache.jackrabbit.oak.spi.lifecycle;

import java.util.Arrays;
import java.util.Collection;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;

/**
 * Composite repository initializer that delegates the
 * {@link #initialize(org.apache.jackrabbit.oak.spi.state.NodeBuilder, String)}
 * calls in sequence to all the component initializers.
 */
public class CompositeWorkspaceInitializer implements WorkspaceInitializer {

    private final Collection<WorkspaceInitializer> initializers;

    public CompositeWorkspaceInitializer(@NotNull Collection<WorkspaceInitializer> trackers) {
        this.initializers = trackers;
    }

    public CompositeWorkspaceInitializer(@NotNull WorkspaceInitializer... initializers) {
        this.initializers = Arrays.asList(initializers);
    }

    @Override
    public void initialize(NodeBuilder builder, String workspaceName) {
        for (WorkspaceInitializer tracker : initializers) {
            tracker.initialize(builder, workspaceName);
        }

    }
}
