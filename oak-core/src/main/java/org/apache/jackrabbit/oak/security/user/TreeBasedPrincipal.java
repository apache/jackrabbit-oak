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
package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.jetbrains.annotations.NotNull;

import javax.jcr.RepositoryException;

/**
 * TreeBasedPrincipal...
 */
class TreeBasedPrincipal extends PrincipalImpl implements ItemBasedPrincipal {

    private final String path;
    private final NamePathMapper pathMapper;

    TreeBasedPrincipal(@NotNull String principalName, @NotNull Tree tree, @NotNull NamePathMapper pathMapper) {
        this(principalName, tree.getPath(), pathMapper);
    }

    TreeBasedPrincipal(@NotNull String principalName, @NotNull String oakPath, @NotNull NamePathMapper pathMapper) {
        super(principalName);
        this.pathMapper = pathMapper;
        this.path = oakPath;
    }

    @NotNull
    String getOakPath() throws RepositoryException {
        return path;
    }

    @NotNull
    NamePathMapper getNamePathMapper() {
        return pathMapper;
    }

    //-------------------------------------------------< ItemBasedPrincipal >---
    @NotNull
    @Override
    public String getPath() throws RepositoryException {
        return pathMapper.getJcrPath(path);
    }
}