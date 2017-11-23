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

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;

public interface EffectiveNodeType {
    boolean includesNodeType(String nodeTypeName);

    boolean includesNodeTypes(String[] nodeTypeNames);

    boolean supportsMixin(String mixin);

    Iterable<NodeDefinition> getNodeDefinitions();

    Iterable<PropertyDefinition> getPropertyDefinitions();

    Iterable<NodeDefinition> getAutoCreateNodeDefinitions();

    Iterable<PropertyDefinition> getAutoCreatePropertyDefinitions();

    Iterable<NodeDefinition> getMandatoryNodeDefinitions();

    Iterable<PropertyDefinition> getMandatoryPropertyDefinitions();

    @Nonnull
    Iterable<NodeDefinition> getNamedNodeDefinitions(
            String oakName);

    @Nonnull
    Iterable<PropertyDefinition> getNamedPropertyDefinitions(
            String oakName);

    @Nonnull
    Iterable<NodeDefinition> getResidualNodeDefinitions();

    @Nonnull
    Iterable<PropertyDefinition> getResidualPropertyDefinitions();

    void checkSetProperty(PropertyState property) throws RepositoryException;

    void checkRemoveProperty(PropertyState property) throws RepositoryException;

    void checkMandatoryItems(Tree tree) throws ConstraintViolationException;

    void checkOrderableChildNodes() throws UnsupportedRepositoryOperationException;

    PropertyDefinition getPropertyDefinition(
            String propertyName, boolean isMultiple,
            int type, boolean exactTypeMatch)
            throws ConstraintViolationException;

    PropertyDefinition getPropertyDefinition(String name, int type, boolean unknownMultiple);

    NodeDefinition getNodeDefinition(
            String childName, EffectiveNodeType childEffective)
            throws ConstraintViolationException;
}
