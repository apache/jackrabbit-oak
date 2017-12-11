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
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.osgi.annotation.versioning.ProviderType;

/**
 * DefinitionProvider... TODO
 */
@ProviderType
public interface DefinitionProvider {

    @Nonnull
    NodeDefinition getRootDefinition() throws RepositoryException;

    /**
     * Returns the node definition for a child node of {@code parent} named
     * {@code nodeName} with a default primary type. First the non-residual
     * child node definitions of {@code parent} are checked matching the
     * given node name. Then the residual definitions are checked.
     *
     * @param parent   the parent node.
     * @param nodeName The internal oak name of the child node.
     * @return the applicable node definition.
     * @throws ConstraintViolationException If no matching definition can be found.
     * @throws RepositoryException If another error occurs.
     */
    @Nonnull
    NodeDefinition getDefinition(@Nonnull Tree parent, @Nonnull String nodeName)
            throws ConstraintViolationException, RepositoryException;

    /**
     * Calculates the applicable definition for the child node under the given
     * parent node.
     *
     * @param parent The parent node.
     * @param targetNode The child node for which the definition is calculated.
     * @return the definition of the target node.
     * @throws ConstraintViolationException If no matching definition can be found.
     * @throws RepositoryException If another error occurs.
     */
    @Nonnull
    NodeDefinition getDefinition(Tree parent, Tree targetNode)
            throws ConstraintViolationException, RepositoryException;

    /**
     * Calculates the applicable definition for the property state under the
     * given parent tree.
     *
     * @param parent The parent tree.
     * @param propertyState The target property.
     * @return the definition for the target property.
     * @throws ConstraintViolationException If no matching definition can be found.
     * @throws RepositoryException If another error occurs.
     */
    @Nonnull
    PropertyDefinition getDefinition(
            Tree parent, PropertyState propertyState, boolean exactTypeMatch)
            throws ConstraintViolationException, RepositoryException;

}