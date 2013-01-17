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

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeIterator;

import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.util.NodeUtil;

import com.google.common.collect.Lists;

/**
 * {@code ReadWriteNodeTypeManager} extends the {@link ReadOnlyNodeTypeManager}
 * and add support for operations that modify node types:
 * <ul>
 *     <li>{@link #registerNodeType(NodeTypeDefinition, boolean)}</li>
 *     <li>{@link #registerNodeTypes(NodeTypeDefinition[], boolean)}</li>
 *     <li>{@link #unregisterNodeType(String)}</li>
 *     <li>{@link #unregisterNodeTypes(String[])}</li>
 * </ul>
 * Calling any of the above methods will result in a {@link #refresh()} callback
 * to e.g. inform an associated session that it should refresh to make the
 * changes visible.
 * </p>
 * Subclass responsibility is to provide an implementation of
 * {@link #getTypes()} for read only access to the tree where node types are
 * stored in content and {@link #getWriteRoot()} for write access to the
 * repository in order to modify node types stored in content. A subclass may
 * also want to override the default implementation of
 * {@link ReadOnlyNodeTypeManager} for the following methods:
 * <ul>
 *     <li>{@link #getValueFactory()}</li>
 *     <li>{@link ReadOnlyNodeTypeManager#getNamePathMapper()}</li>
 * </ul>
 */
public abstract class ReadWriteNodeTypeManager extends ReadOnlyNodeTypeManager {

    /**
     * Called by the methods {@link #registerNodeType(NodeTypeDefinition,boolean)},
     * {@link #registerNodeTypes(NodeTypeDefinition[], boolean)},
     * {@link #unregisterNodeType(String)} and {@link #unregisterNodeTypes(String[])}
     * to acquire a fresh {@link Root} instance that can be used to persist the
     * requested node type changes (and nothing else).
     * <p/>
     * This default implementation throws an {@link UnsupportedOperationException}.
     *
     * @return fresh {@link Root} instance.
     */
    @Nonnull
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

    @Override
    public NodeType registerNodeType(NodeTypeDefinition ntd, boolean allowUpdate) throws RepositoryException {
        // TODO proper node type registration... (OAK-66, OAK-411)
        Root root = getWriteRoot();
        Tree types = getOrCreateNodeTypes(root);
        try {
            NodeType type = internalRegister(types, ntd, allowUpdate);
            root.commit();
            refresh();
            return type;
        } catch (CommitFailedException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    public final NodeTypeIterator registerNodeTypes(NodeTypeDefinition[] ntds, boolean allowUpdate)
            throws RepositoryException {
        // TODO handle inter-type dependencies (OAK-66, OAK-411)
        Root root = getWriteRoot();
        try {
            List<NodeType> list = internalRegister(
                    root, Arrays.asList(ntds), allowUpdate);
            root.commit();
            refresh();
            return new NodeTypeIteratorAdapter(list);
        } catch (CommitFailedException e) {
            throw new RepositoryException(e);
        }
    }

    private List<NodeType> internalRegister(
            Root root, Iterable<? extends NodeTypeDefinition> ntds,
            boolean allowUpdate) throws RepositoryException {
        Tree types = getOrCreateNodeTypes(root);
        List<NodeType> list = Lists.newArrayList();
        for (NodeTypeDefinition ntd : ntds) {
            list.add(internalRegister(types, ntd, allowUpdate));
        }
        return list;
    }

    private NodeType internalRegister(
            Tree types, NodeTypeDefinition ntd, boolean allowUpdate)
            throws RepositoryException {
        NodeTypeTemplateImpl template;
        if (ntd instanceof NodeTypeTemplateImpl) {
            template = (NodeTypeTemplateImpl) ntd;
        } else {
            // some external template implementation, copy before proceeding
            template = new NodeTypeTemplateImpl(getNamePathMapper(), ntd);
        }
        Tree type = template.writeTo(types, allowUpdate);
        NodeUtil node = new NodeUtil(type, getNamePathMapper());
        return new NodeTypeImpl(this, getValueFactory(), node);
    }

    private static Tree getOrCreateNodeTypes(Root root) {
        Tree types = root.getTree(NODE_TYPES_PATH);
        if (types == null) {
            Tree system = root.getTree('/' + JCR_SYSTEM);
            if (system == null) {
                system = root.getTree("/").addChild(JCR_SYSTEM);
            }
            types = system.addChild(JCR_NODE_TYPES);
        }
        return types;
    }

    @Override
    public void unregisterNodeType(String name) throws RepositoryException {
        Tree type = null;
        Root root = getWriteRoot();
        Tree types = root.getTree(NODE_TYPES_PATH);
        if (types != null) {
            type = types.getChild(getOakName(name));
        }
        if (type == null) {
            throw new NoSuchNodeTypeException("Node type " + name + " can not be unregistered.");
        }

        try {
            type.remove();
            root.commit();
            refresh();
        } catch (CommitFailedException e) {
            throw new RepositoryException("Failed to unregister node type " + name, e);
        }
    }

    @Override
    public void unregisterNodeTypes(String[] names) throws RepositoryException {
        Root root = getWriteRoot();
        Tree types = root.getTree(NODE_TYPES_PATH);
        if (types == null) {
            throw new NoSuchNodeTypeException("Node types can not be unregistered.");
        }

        try {
            for (String name : names) {
                Tree type = types.getChild(getOakName(name));
                if (type == null) {
                    throw new NoSuchNodeTypeException("Node type " + name + " can not be unregistered.");
                }
                type.remove();
            }
            root.commit();
            refresh();
        } catch (CommitFailedException e) {
            throw new RepositoryException("Failed to unregister node types", e);
        }
    }
}
