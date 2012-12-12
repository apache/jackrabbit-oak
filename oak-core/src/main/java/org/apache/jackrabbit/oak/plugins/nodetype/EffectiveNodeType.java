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
package org.apache.jackrabbit.oak.plugins.nodetype;

import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;

/**
 * EffectiveNodeType... TODO
 */
public interface EffectiveNodeType {

    Iterable<NodeType> getAllNodeTypes();

    //Iterable<NodeType> getInheritedNodeTypes();
    //Iterable<NodeType> getMergedNodeTypes();

    /**
     * Determines whether this effective node type representation includes
     * (either through inheritance or aggregation) the given node type.
     *
     * @param nodeTypeName name of node type
     * @return {@code true} if the given node type is included, otherwise {@code false}.
     */
    boolean includesNodeType(String nodeTypeName);

    /**
     * Determines whether this effective node type representation includes
     * (either through inheritance or aggregation) all of the given node types.
     *
     * @param nodeTypeNames array of node type names
     * @return {@code true} if all of the given node types are included,
     *         otherwise {@code false}
     */
    boolean includesNodeTypes(String[] nodeTypeNames);

    /**
     * Determines whether this effective node type supports adding
     * the specified mixin.
     * @param mixin name of mixin type
     * @return {@code true} if the mixin type is supported, otherwise {@code false}
     */
    boolean supportsMixin(String mixin);

    Iterable<NodeDefinition> getNodeDefinitions();

    Iterable<PropertyDefinition> getPropertyDefinitions();

    Iterable<NodeDefinition> getAutoCreateNodeDefinitions();

    Iterable<PropertyDefinition> getAutoCreatePropertyDefinitions();

    Iterable<NodeDefinition> getMandatoryNodeDefinitions();

    Iterable<PropertyDefinition> getMandatoryPropertyDefinitions();

    Iterable<NodeDefinition> getNamedNodeDefinitions(String name);

    Iterable<PropertyDefinition> getNamedPropertyDefinitions(String name);

    Iterable<NodeDefinition> getUnnamedNodeDefinitions();

    Iterable<PropertyDefinition> getUnnamedPropertyDefinitions();

    void checkSetProperty(PropertyState property) throws RepositoryException;

    void checkRemoveProperty(PropertyState property) throws RepositoryException;

    void checkAddChildNode(String name, NodeType nodeType) throws RepositoryException;

    void checkRemoveNode(String name, NodeType nodeType) throws RepositoryException;

    void checkMandatoryItems(Tree tree) throws ConstraintViolationException;
}
