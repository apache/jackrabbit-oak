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
package org.apache.jackrabbit.oak.spi.nodetype;

import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

@ProviderType
public interface EffectiveNodeType {
    boolean includesNodeType(@NotNull String nodeTypeName);

    boolean includesNodeTypes(@NotNull String[] nodeTypeNames);

    boolean supportsMixin(@NotNull String mixin);

    @NotNull
    Iterable<NodeDefinition> getNodeDefinitions();

    @NotNull
    Iterable<PropertyDefinition> getPropertyDefinitions();

    @NotNull
    Iterable<NodeDefinition> getAutoCreateNodeDefinitions();

    @NotNull
    Iterable<PropertyDefinition> getAutoCreatePropertyDefinitions();

    @NotNull
    Iterable<NodeDefinition> getMandatoryNodeDefinitions();

    @NotNull
    Iterable<PropertyDefinition> getMandatoryPropertyDefinitions();

    @NotNull
    Iterable<NodeDefinition> getNamedNodeDefinitions(@NotNull String oakName);

    @NotNull
    Iterable<PropertyDefinition> getNamedPropertyDefinitions(@NotNull String oakName);

    @NotNull
    Iterable<NodeDefinition> getResidualNodeDefinitions();

    @NotNull
    Iterable<PropertyDefinition> getResidualPropertyDefinitions();

    void checkSetProperty(@NotNull PropertyState property) throws RepositoryException;

    void checkRemoveProperty(@NotNull PropertyState property) throws RepositoryException;

    void checkMandatoryItems(@NotNull Tree tree) throws ConstraintViolationException;

    void checkOrderableChildNodes() throws UnsupportedRepositoryOperationException;

    @NotNull
    PropertyDefinition getPropertyDefinition(@NotNull String propertyName, boolean isMultiple, int type, boolean exactTypeMatch) throws ConstraintViolationException;

    @Nullable
    PropertyDefinition getPropertyDefinition(@NotNull String name, int type, boolean unknownMultiple);

    @NotNull
    NodeDefinition getNodeDefinition(@NotNull String childName, @Nullable EffectiveNodeType childEffective) throws ConstraintViolationException;
}
