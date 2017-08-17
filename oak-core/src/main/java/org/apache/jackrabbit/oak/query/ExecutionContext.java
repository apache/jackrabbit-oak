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

package org.apache.jackrabbit.oak.query;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * An instance of this class provides the context for the execution of a query,
 * which in essence captures a stable state of the content tree from the time
 * the execution context was created.
 */
public class ExecutionContext {

    /**
     * Base state used for index lookups.
     */
    private final NodeState baseState;

    private final Root root;

    private final QueryEngineSettings settings;
    
    private final QueryIndexProvider indexProvider;

    private final PermissionProvider permissionProvider;

    public ExecutionContext(
            NodeState baseState, Root root,
            QueryEngineSettings settings,
            QueryIndexProvider indexProvider, PermissionProvider permissionProvider) {
        this.baseState = baseState;
        this.root = root;
        this.settings = settings;
        this.indexProvider = indexProvider;
        this.permissionProvider = permissionProvider;
    }

    /**
     * Used to evaluate the query (ie. read the existing node types, index
     * definitions), doesn't need to be a secured version of a node state
     * 
     * @return base state of the content tree against which the query runs.
     */
    @Nonnull
    public NodeState getBaseState() {
        return baseState;
    }
    
    /**
     * Get the nodetype info provider.
     * 
     * @return the provider
     */
    public NodeTypeInfoProvider getNodeTypeInfoProvider() {
        return new NodeStateNodeTypeInfoProvider(baseState);
    }

    /**
     * Used to create the actual query results from the indexed paths, needs to
     * be a secured version of a tree to take into account ACLs
     * 
     * @return root of the content tree against which the query runs.
     */
    @Nonnull
    public Root getRoot() {
        return root;
    }

    /**
     * @return Index provider for indexes matching the state of the content tree as
     * returned from {@link #getBaseState()}.
     */
    @Nonnull
    public QueryIndexProvider getIndexProvider() {
        return indexProvider;
    }
    
    public QueryEngineSettings getSettings() {
        return settings;
    }

    @CheckForNull
    public PermissionProvider getPermissionProvider() {
        return permissionProvider;
    }
}
