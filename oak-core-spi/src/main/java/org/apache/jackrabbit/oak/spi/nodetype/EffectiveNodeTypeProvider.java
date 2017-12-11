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

import java.util.Iterator;
import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NoSuchNodeTypeException;

import org.apache.jackrabbit.oak.api.Tree;
import org.osgi.annotation.versioning.ProviderType;

/**
 * EffectiveNodeTypeProvider... TODO
 */
@ProviderType
public interface EffectiveNodeTypeProvider {

    /**
     * Returns {@code true} if this tree is of the specified primary node
     * type or mixin type, or a subtype thereof respecting the effective node
     * type of the {@code tree}. Returns {@code false} otherwise.
     *
     * @param tree The tree to be tested.
     * @param nodeTypeName The internal oak name of the node type to be tested.
     * @return true if the specified node is of the given node type.
     */
    boolean isNodeType(Tree tree, String nodeTypeName);

    /**
     * Returns {@code true} if {@code typeName} is of the specified primary node
     * type or mixin type, or a subtype thereof. Returns {@code false} otherwise.
     *
     * @param primaryTypeName  the internal oak name of the node to test
     * @param mixinTypes the internal oak names of the node to test.
     * @param nodeTypeName The internal oak name of the node type to be tested.
     * @return {@code true} if the specified node type is of the given node type.
     * @throws NoSuchNodeTypeException If the specified node type name doesn't
     * refer to an existing node type.
     * @throws RepositoryException If the given node type name is invalid or if
     * some other error occurs.
     */
    boolean isNodeType(@Nonnull String primaryTypeName, @Nonnull Iterator<String> mixinTypes, @Nonnull String nodeTypeName) throws NoSuchNodeTypeException, RepositoryException;

    /**
     * Returns {@code true} if {@code typeName} is of the specified primary node
     * type or mixin type, or a subtype thereof. Returns {@code false} otherwise.
     *
     * @param typeName  the internal oak name of the node type to test
     * @param superName The internal oak name of the super type to be tested for.
     * @return {@code true} if the specified node type is of the given node type.
     */
    boolean isNodeType(String typeName, String superName);

    /**
     * Calculates and returns the effective node types of the given node.
     * Also see <a href="http://www.jcp.org/en/jsr/detail?id=283">JCR 2.0 Specification, Section 3.7.6.5</a>
     * for the definition of the effective node type.
     *
     * @param targetNode the node for which the types should be calculated.
     * @return all types of the given node
     * @throws RepositoryException if the type information can not be accessed
     * @see <a href="http://www.jcp.org/en/jsr/detail?id=283">JCR 2.0 Specification, Section 3.7.6.5</a>
     */
    EffectiveNodeType getEffectiveNodeType(Node targetNode) throws RepositoryException;

    /**
     * Calculates and returns the effective node types of the given tree.
     * Also see <a href="http://www.jcp.org/en/jsr/detail?id=283">JCR 2.0 Specification, Section 3.7.6.5</a>
     * for the definition of the effective node type.
     *
     * @param tree the tree
     * @return all node types of the given tree
     * @throws RepositoryException if the type information can not be accessed,
     * @see <a href="http://www.jcp.org/en/jsr/detail?id=283">JCR 2.0 Specification, Section 3.7.6.5</a>
     */
    EffectiveNodeType getEffectiveNodeType(Tree tree) throws RepositoryException;
}