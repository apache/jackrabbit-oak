/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.security;

import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.jetbrains.annotations.NotNull;

/**
 * Abstract base implementation for the various security configurations.
 */
public abstract class ConfigurationBase extends SecurityConfiguration.Default {

    private SecurityProvider securityProvider;

    private ConfigurationParameters config = ConfigurationParameters.EMPTY;

    private RootProvider rootProvider;

    private TreeProvider treeProvider;

    /**
     * osgi constructor
     */
    public ConfigurationBase() {
    }

    /**
     * non-osgi constructor
     */
    public ConfigurationBase(@NotNull SecurityProvider securityProvider, @NotNull ConfigurationParameters config) {
        this.securityProvider = securityProvider;
        this.config = config;
    }

    @NotNull
    public SecurityProvider getSecurityProvider() {
        if (securityProvider == null) {
            throw new IllegalStateException();
        }
        return securityProvider;
    }

    public void setSecurityProvider(@NotNull SecurityProvider securityProvider) {
        this.securityProvider = securityProvider;
    }

    public void setParameters(@NotNull ConfigurationParameters config) {
        this.config = config;
    }

    public void setRootProvider(@NotNull RootProvider rootProvider) {
        this.rootProvider = rootProvider;
    }

    @NotNull
    public RootProvider getRootProvider() {
        if (rootProvider == null) {
            throw new IllegalStateException("RootProvider missing.");
        }
        return rootProvider;
    }

    public void setTreeProvider(@NotNull TreeProvider treeProvider) {
        this.treeProvider = treeProvider;
    }

    @NotNull
    public TreeProvider getTreeProvider() {
        if (treeProvider == null) {
            throw new IllegalStateException("TreeProvider missing.");
        }
        return treeProvider;
    }

    //----------------------------------------------< SecurityConfiguration >---
    @NotNull
    @Override
    public ConfigurationParameters getParameters() {
        return config;
    }

}
