/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.type;

import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinitionTemplate;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.memory.MemoryValueFactory;
import org.apache.jackrabbit.oak.util.NodeUtil;

/**
 * Base implementation of a {@link NodeTypeManager} with support for reading
 * node types from the {@link Tree} returned by {@link #getTypes()}. Methods
 * related to node type modifications throw
 * {@link UnsupportedRepositoryOperationException}.
 */
public abstract class ReadOnlyNodeTypeManager implements NodeTypeManager {

    /**
     * Returns the internal name for the specified JCR name.
     *
     * @param jcrName JCR node type name.
     * @return the internal representation of the given JCR name.
     * @throws javax.jcr.RepositoryException If there is no valid internal representation
     * of the specified JCR name.
     */
    @Nonnull
    protected final String getOakName(String jcrName) throws RepositoryException {
        String oakName = getNameMapper().getOakName(jcrName);
        if (oakName == null) {
            throw new RepositoryException("Invalid JCR name " + jcrName);
        }
        return oakName;
    }

    /**
     * @return  {@link org.apache.jackrabbit.oak.api.Tree} instance where the node types
     * are stored or {@code null} if none.
     */
    @CheckForNull
    protected abstract Tree getTypes();

    /**
     * Called by the {@link NodeTypeManager} implementation methods to
     * refresh the state of the session associated with this instance.
     * That way the session is kept in sync with the latest global state
     * seen by the node type manager.
     *
     * @throws RepositoryException if the session could not be refreshed
     */
    protected void refresh() throws RepositoryException {
    }

    /**
     * The value factory to be used by {@link org.apache.jackrabbit.oak.plugins.type.PropertyDefinitionImpl#getDefaultValues()}.
     * If {@code null} the former returns {@code null}.
     * @return  {@code ValueFactory} instance or {@code null}.
     */
    @CheckForNull
    protected ValueFactory getValueFactory() {
        return null;
    }

    /**
     * Returns a {@link CoreValueFactory} to be used by this node type manager.
     * This implementation returns a {@link MemoryValueFactory#INSTANCE}. A
     * subclass may override this method and provide a different
     * implementation.
     *
     * @return {@link CoreValueFactory} instance.
     */
    @Nonnull
    protected CoreValueFactory getCoreValueFactory() {
        return MemoryValueFactory.INSTANCE;
    }

    /**
     * Returns a {@link NameMapper} to be used by this node type manager. This
     * implementation returns the {@link NamePathMapperImpl#DEFAULT} instance. A
     * subclass may override this method and provide a different
     * implementation.
     *
     * @return {@link NameMapper} instance.
     */
    @Nonnull
    protected NameMapper getNameMapper() {
        return NamePathMapperImpl.DEFAULT;
    }

    //----------------------------------------------------< NodeTypeManager >---

    @Override
    public boolean hasNodeType(String name) throws RepositoryException {
        Tree types = getTypes();
        return types != null && types.hasChild(getOakName(name));
    }

    @Override
    public NodeType getNodeType(String name) throws RepositoryException {
        Tree types = getTypes();
        if (types != null) {
            Tree type = types.getChild(getOakName(name));
            if (type != null) {
                return new NodeTypeImpl(this, getValueFactory(),
                        new NodeUtil(type, getCoreValueFactory(), getNameMapper()));
            }
        }
        throw new NoSuchNodeTypeException(name);
    }

    @Override
    public NodeTypeIterator getAllNodeTypes() throws RepositoryException {
        List<NodeType> list = Lists.newArrayList();
        Tree types = getTypes();
        if (types != null) {
            for (Tree type : types.getChildren()) {
                list.add(new NodeTypeImpl(this, getValueFactory(),
                        new NodeUtil(type, getCoreValueFactory(), getNameMapper())));

            }
        }
        return new NodeTypeIteratorAdapter(list);
    }

    @Override
    public NodeTypeIterator getPrimaryNodeTypes() throws RepositoryException {
        List<NodeType> list = Lists.newArrayList();
        NodeTypeIterator iterator = getAllNodeTypes();
        while (iterator.hasNext()) {
            NodeType type = iterator.nextNodeType();
            if (!type.isMixin()) {
                list.add(type);
            }
        }
        return new NodeTypeIteratorAdapter(list);
    }

    @Override
    public NodeTypeIterator getMixinNodeTypes() throws RepositoryException {
        List<NodeType> list = Lists.newArrayList();
        NodeTypeIterator iterator = getAllNodeTypes();
        while (iterator.hasNext()) {
            NodeType type = iterator.nextNodeType();
            if (type.isMixin()) {
                list.add(type);
            }
        }
        return new NodeTypeIteratorAdapter(list);
    }

    @Override
    public NodeTypeTemplate createNodeTypeTemplate() throws RepositoryException {
        return new NodeTypeTemplateImpl(this, getNameMapper(), getValueFactory());
    }

    @Override
    public NodeTypeTemplate createNodeTypeTemplate(NodeTypeDefinition ntd) throws RepositoryException {
        return new NodeTypeTemplateImpl(this, getNameMapper(), getValueFactory(), ntd);
    }

    @Override
    public NodeDefinitionTemplate createNodeDefinitionTemplate() {
        return new NodeDefinitionTemplateImpl(getNameMapper());
    }

    @Override
    public PropertyDefinitionTemplate createPropertyDefinitionTemplate() {
        return new PropertyDefinitionTemplateImpl(getNameMapper());
    }

    /**
     * This implementation always throws a {@link UnsupportedRepositoryOperationException}.
     */
    @Override
    public NodeType registerNodeType(NodeTypeDefinition ntd, boolean allowUpdate) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    /**
     * This implementation always throws a {@link UnsupportedRepositoryOperationException}.
     */
    @Override
    public NodeTypeIterator registerNodeTypes(NodeTypeDefinition[] ntds, boolean allowUpdate) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    /**
     * This implementation always throws a {@link UnsupportedRepositoryOperationException}.
     */
    @Override
    public void unregisterNodeType(String name) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    /**
     * This implementation always throws a {@link UnsupportedRepositoryOperationException}.
     */
    @Override
    public void unregisterNodeTypes(String[] names) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }
}
