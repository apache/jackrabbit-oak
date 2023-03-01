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
package org.apache.jackrabbit.oak.plugins.nodetype.write;

import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.jetbrains.annotations.NotNull;

import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeExistsException;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinitionTemplate;
import java.util.ArrayList;
import java.util.List;

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

/**
 * {@code ReadWriteNodeTypeManager} extends the {@link ReadOnlyNodeTypeManager}
 * with support for operations that modify node types.
 * <ul>
 * <li>{@link #registerNodeType(NodeTypeDefinition, boolean)}</li>
 * <li>{@link #registerNodeTypes(NodeTypeDefinition[], boolean)}</li>
 * <li>{@link #unregisterNodeType(String)}</li>
 * <li>{@link #unregisterNodeTypes(String[])}</li>
 * <li>plus related template factory methods</li>
 * </ul>
 * Calling any of the above methods will result in a {@link #refresh()} callback
 * to e.g. inform an associated session that it should refresh to make the
 * changes visible.
 * <p>
 * Subclass responsibility is to provide an implementation of
 * {@link #getTypes()} for read only access to the tree where node types are
 * stored in content and {@link #getWriteRoot()} for write access to the
 * repository in order to modify node types stored in content. A subclass may
 * also want to override the default implementation of
 * {@link ReadOnlyNodeTypeManager} for the following methods:
 * <ul>
 * <li>{@link #getValueFactory()}</li>
 * <li>{@link ReadOnlyNodeTypeManager#getNamePathMapper()}</li>
 * </ul>
 */
public abstract class ReadWriteNodeTypeManager extends ReadOnlyNodeTypeManager {

    /**
     * Called by the methods {@link #registerNodeType(NodeTypeDefinition, boolean)},
     * {@link #registerNodeTypes(NodeTypeDefinition[], boolean)},
     * {@link #unregisterNodeType(String)} and {@link #unregisterNodeTypes(String[])}
     * to acquire a fresh {@link Root} instance that can be used to persist the
     * requested node type changes (and nothing else).
     * <p>
     * This default implementation throws an {@link UnsupportedOperationException}.
     *
     * @return fresh {@link Root} instance.
     */
    @NotNull
    protected Root getWriteRoot() {
        throw new UnsupportedOperationException();
    }

    /**
     * Called by the {@link ReadWriteNodeTypeManager} implementation methods to
     * refresh the state of the session associated with this instance.
     * That way the session is kept in sync with the latest global state
     * seen by the node type manager.
     *
     * @throws RepositoryException if the session could not be refreshed
     */
    protected void refresh() throws RepositoryException {
    }

    //----------------------------------------------------< NodeTypeManager >---
    /**
     * Returns an empty {@code NodeTypeTemplate} which can then be used to
     * define a node type and passed to {@code NodeTypeManager.registerNodeType}.
     *
     * @return A new empty {@code NodeTypeTemplate}.
     * @since JCR 2.0
     */
    @Override
    public NodeTypeTemplate createNodeTypeTemplate() {
        return new NodeTypeTemplateImpl(getNamePathMapper());
    }

    /**
     * Returns a {@code NodeTypeTemplate} from the given definition, which can then be used to
     * define a node type and passed to {@code NodeTypeManager.registerNodeType}.
     *
     * @return A new {@code NodeTypeTemplate}.
     * @since JCR 2.0
     */
    @Override
    public NodeTypeTemplate createNodeTypeTemplate(NodeTypeDefinition ntd)
            throws ConstraintViolationException {
        return new NodeTypeTemplateImpl(getNamePathMapper(), ntd);
    }

    /**
     * Returns an empty {@code NodeDefinitionTemplate} which can then be
     * used to create a child node definition and attached to a
     * {@code NodeTypeTemplate}.
     *
     * @return A new {@code NodeDefinitionTemplate}.
     * @since JCR 2.0
     */
    @Override
    public NodeDefinitionTemplate createNodeDefinitionTemplate() {
        return new NodeDefinitionTemplateImpl(getNamePathMapper());
    }

    /**
     * Returns an empty {@code PropertyDefinitionTemplate} which can then
     * be used to create a property definition and attached to a
     * {@code NodeTypeTemplate}.
     *
     * @return A new {@code PropertyDefinitionTemplate}.
     * @since JCR 2.0
     */
    @Override
    public PropertyDefinitionTemplate createPropertyDefinitionTemplate() {
        return new PropertyDefinitionTemplateImpl(getNamePathMapper());
    }

    /**
     * Registers a new node type or updates an existing node type using the specified definition and returns the 
     * resulting {@code NodeType} object.
     * <p>
     * Typically, the object passed to this method will be a {@code NodeTypeTemplate} (a subclass of {@code NodeTypeDefinition})
     * acquired from {@code NodeTypeManager.createNodeTypeTemplate} and then filled-in with definition information.
     *
     * @param ntd an {@code NodeTypeDefinition}.
     * @param allowUpdate a boolean
     * @return the registered node type
     * @throws NodeTypeExistsException if {@code allowUpdate} is {@code false} and the {@code NodeTypeDefinition} 
     * specifies a node type name that is already registered.
     * @throws RepositoryException if another error occurs.
     * @since JCR 2.0
     */
    @Override
    public NodeType registerNodeType(
            NodeTypeDefinition ntd, boolean allowUpdate)
            throws RepositoryException {
        return registerNodeTypes(
                new NodeTypeDefinition[]{ntd}, allowUpdate).nextNodeType();
    }

    /**
     * Registers or updates the specified array of {@code NodeTypeDefinition} objects. This method is used to register 
     * or update a set of node types with mutual dependencies. Returns an iterator over the resulting {@code NodeType} objects.
     * <p>
     * The effect of the method is "all or nothing"; if an error occurs, no node
     * types are registered or updated.
     *
     * @param ntds a collection of {@code NodeTypeDefinition}s
     * @param allowUpdate a boolean
     * @return the registered node types.
     * @throws NodeTypeExistsException if {@code allowUpdate} is {@code false} and a {@code NodeTypeDefinition} within 
     * the {@code Collection} specifies a node type name that is already registered.
     * @throws RepositoryException if another error occurs.
     * @since JCR 2.0
     */
    @Override
    public final NodeTypeIterator registerNodeTypes(
            NodeTypeDefinition[] ntds, boolean allowUpdate)
            throws RepositoryException {
        Root root = getWriteRoot();
        try {
            Tree tree = getOrCreateNodeTypes(root);
            for (NodeTypeDefinition ntd : ntds) {
                NodeTypeTemplateImpl template;
                if (ntd instanceof NodeTypeTemplateImpl) {
                    template = (NodeTypeTemplateImpl) ntd;
                } else {
                    // some external template implementation, copy before proceeding
                    template = new NodeTypeTemplateImpl(getNamePathMapper(), ntd);
                }
                template.writeTo(tree, allowUpdate);
            }
            root.commit();

            refresh();

            List<NodeType> types = new ArrayList<>(ntds.length);
            for (NodeTypeDefinition ntd : ntds) {
                types.add(getNodeType(ntd.getName()));
            }
            return new NodeTypeIteratorAdapter(types);
        } catch (CommitFailedException e) {
            String message = "Failed to register node types.";
            throw e.asRepositoryException(message);
        }
    }

    private static @NotNull Tree getOrCreateNodeTypes(@NotNull Root root) {
        Tree types = root.getTree(NODE_TYPES_PATH);
        if (!types.exists()) {
            Tree system = root.getTree('/' + JCR_SYSTEM);
            if (!system.exists()) {
                system = root.getTree("/").addChild(JCR_SYSTEM);
            }
            types = system.addChild(JCR_NODE_TYPES);
        }
        return types;
    }

    /**
     * Unregisters the specified node type.
     *
     * @param name a {@code String}.
     * @throws NoSuchNodeTypeException if no registered node type exists with the specified name.
     * @throws RepositoryException if another error occurs.
     * @since JCR 2.0
     */
    @Override
    public void unregisterNodeType(String name) throws RepositoryException {
        Root root = getWriteRoot();
        Tree type = root.getTree(NODE_TYPES_PATH).getChild(getOakName(name));
        if (!type.exists()) {
            throw new NoSuchNodeTypeException("Node type " + name + " can not be unregistered.");
        }

        try {
            type.remove();
            root.commit();
            refresh();
        } catch (CommitFailedException e) {
            String message = "Failed to unregister node type " + name;
            throw e.asRepositoryException(message);
        }
    }

    /**
     * Unregisters the specified set of node types. Used to unregister a set of
     * node types with mutual dependencies.
     *
     * @param names a {@code String} array
     * @throws NoSuchNodeTypeException if one of the names listed is not a registered node type.
     * @throws RepositoryException if another error occurs.
     * @since JCR 2.0
     */
    @Override
    public void unregisterNodeTypes(String[] names) throws RepositoryException {
        Root root = getWriteRoot();
        Tree types = root.getTree(NODE_TYPES_PATH);
        if (!types.exists()) {
            throw new NoSuchNodeTypeException("Node types can not be unregistered.");
        }

        try {
            for (String name : names) {
                Tree type = types.getChild(getOakName(name));
                if (!type.exists()) {
                    throw new NoSuchNodeTypeException("Node type " + name + " can not be unregistered.");
                }
                type.remove();
            }
            root.commit();
            refresh();
        } catch (CommitFailedException e) {
            String message = "Failed to unregister node types.";
            throw e.asRepositoryException(message);
        }
    }
}
