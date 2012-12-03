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

import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;

/**
 * DefinitionProvider... TODO
 */
public interface DefinitionProvider {

    @Nonnull
    NodeDefinition getRootDefinition() throws RepositoryException;

    /**
     * Returns the node definition for a child node of <code>parent</code> named
     * <code>nodeName</code> with a default primary type. First the non-residual
     * child node definitions of <code>parent</code> are checked matching the
     * given node name. Then the residual definitions are checked.
     *
     * @param parent   the parent node.
     * @param nodeName the name of the child node.
     * @return the applicable node definition.
     * @throws RepositoryException if there is no applicable node definition
     *                             with a default primary type.
     */
    @Nonnull
    NodeDefinition getDefinition(@Nonnull Node parent, @Nonnull String nodeName)
            throws RepositoryException;

    @Nonnull
    NodeDefinition getDefinition(Node parent, Node targetNode) throws RepositoryException;

    @Nonnull
    NodeDefinition getDefinition(Iterable<NodeType> parentNodeTypes, String nodeName, NodeType nodeType) throws RepositoryException;

    @Nonnull
    PropertyDefinition getDefinition(Node parent, Property targetProperty) throws RepositoryException;

    @Nonnull
    PropertyDefinition getDefinition(Tree parent, PropertyState propertyState) throws RepositoryException;

    @Nonnull
    PropertyDefinition getDefinition(Node parent, String propertyName, boolean isMultiple, int type, boolean exactTypeMatch) throws RepositoryException;

    @Nonnull
    PropertyDefinition getDefinition(Tree parent, String propertyName, boolean isMultiple, int type, boolean exactTypeMatch) throws RepositoryException;

    @Nonnull
    PropertyDefinition getDefinition(Iterable<NodeType> nodeTypes, String propertyName, boolean isMultiple, int type, boolean exactTypeMatch) throws RepositoryException;
}