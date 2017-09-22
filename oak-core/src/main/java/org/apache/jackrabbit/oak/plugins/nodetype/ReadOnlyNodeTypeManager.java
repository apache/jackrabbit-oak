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
package org.apache.jackrabbit.oak.plugins.nodetype;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.contains;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.dropIndexFromName;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NODE_TYPES_PATH;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_SUPERTYPES;

import java.util.Iterator;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.nodetype.PropertyDefinitionTemplate;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.spi.nodetype.DefinitionProvider;
import org.apache.jackrabbit.oak.spi.nodetype.EffectiveNodeType;
import org.apache.jackrabbit.oak.spi.nodetype.EffectiveNodeTypeProvider;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;

/**
 * Base implementation of a {@link NodeTypeManager} with support for reading
 * node types from the {@link Tree} returned by {@link #getTypes()}. Methods
 * related to node type modifications throw
 * {@link UnsupportedRepositoryOperationException}.
 */
public abstract class ReadOnlyNodeTypeManager implements NodeTypeManager, EffectiveNodeTypeProvider, DefinitionProvider {

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
        return getNamePathMapper().getOakName(jcrName);
    }

    /**
     * @return  {@link org.apache.jackrabbit.oak.api.Tree} instance where the node types
     * are stored or {@code null} if none.
     */
    @CheckForNull
    protected abstract Tree getTypes();

    /**
     * The value factory to be used by {@link org.apache.jackrabbit.oak.plugins.nodetype.PropertyDefinitionImpl#getDefaultValues()}.
     * If {@code null} the former returns {@code null}.
     * @return  {@code ValueFactory} instance or {@code null}.
     */
    @CheckForNull
    protected ValueFactory getValueFactory() {
        return null;
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
    protected NamePathMapper getNamePathMapper() {
        return NamePathMapper.DEFAULT;
    }

    //--------------------------------------------------------------------------

    /**
     * Return a new instance of {@code ReadOnlyNodeTypeManager} that reads node
     * type information from the tree at {@link NodeTypeConstants#NODE_TYPES_PATH}.
     *
     * @param root The root to read node types from.
     * @param namePathMapper The {@code NamePathMapper} to use.
     * @return a new instance of {@code ReadOnlyNodeTypeManager}.
     */
    @Nonnull
    public static ReadOnlyNodeTypeManager getInstance(final Root root,
                                                      final NamePathMapper namePathMapper) {
        return new ReadOnlyNodeTypeManager() {
            @Override
            protected Tree getTypes() {
                return root.getTree(NODE_TYPES_PATH);
            }

            @Nonnull
            @Override
            protected NamePathMapper getNamePathMapper() {
                return namePathMapper;
            }
        };
    }

    //----------------------------------------------------< NodeTypeManager >---

    @Override
    public boolean hasNodeType(String name) throws RepositoryException {
        Tree types = getTypes();
        return types != null && types.hasChild(getOakName(name));
    }

    @Override
    public NodeType getNodeType(String name) throws RepositoryException {
        return internalGetNodeType(getOakName(name));
    }

    @Override
    public NodeTypeIterator getAllNodeTypes() throws RepositoryException {
        List<NodeType> list = Lists.newArrayList();
        Tree types = getTypes();
        if (types != null) {
            NamePathMapper mapper = getNamePathMapper();
            for (Tree type : types.getChildren()) {
                list.add(new NodeTypeImpl(type, mapper));
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
    public NodeTypeTemplate createNodeTypeTemplate()
            throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public NodeTypeTemplate createNodeTypeTemplate(NodeTypeDefinition ntd)
            throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public NodeDefinitionTemplate createNodeDefinitionTemplate()
            throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public PropertyDefinitionTemplate createPropertyDefinitionTemplate()
            throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
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

    //------------------------------------------< EffectiveNodeTypeProvider >---

    @Override
    public boolean isNodeType(Tree tree, String oakNtName) {
        // shortcuts for common cases
        if (JcrConstants.NT_BASE.equals(oakNtName)) {
            return true;
        } else if (JcrConstants.MIX_REFERENCEABLE.equals(oakNtName)
                && !tree.hasProperty(JcrConstants.JCR_UUID)) {
            return false;
        } else if (JcrConstants.MIX_VERSIONABLE.equals(oakNtName)
                && !tree.hasProperty(JcrConstants.JCR_ISCHECKEDOUT)) {
            return false;
        }

        Tree types = getTypes();

        PropertyState primary = tree.getProperty(JcrConstants.JCR_PRIMARYTYPE);
        if (primary != null && primary.getType() == Type.NAME) {
            String name = primary.getValue(Type.NAME);
            if (isa(types, name, oakNtName)) {
                return true;
            }
        }

        PropertyState mixins = tree.getProperty(JcrConstants.JCR_MIXINTYPES);
        if (mixins != null && mixins.getType() == Type.NAMES) {
            for (String name : mixins.getValue(Type.NAMES)) {
                if (isa(types, name, oakNtName)) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public boolean isNodeType(@CheckForNull String primaryTypeName, @Nonnull Iterator<String> mixinTypes,
                              @Nonnull String nodeTypeName) throws NoSuchNodeTypeException, RepositoryException {
        // shortcut
        if (JcrConstants.NT_BASE.equals(nodeTypeName)) {
            return true;
        }
        Tree types = getTypes();
        if (primaryTypeName != null && isa(types, primaryTypeName, nodeTypeName)) {
            return true;
        }
        while (mixinTypes.hasNext()) {
            if (isa(types, mixinTypes.next(), nodeTypeName)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isa(Tree types, String typeName, String superName) {
        if (typeName.equals(superName)) {
            return true;
        }

        Tree type = types.getChild(typeName);
        if (!type.exists()) {
            return false;
        }

        PropertyState supertypes = type.getProperty(REP_SUPERTYPES);
        return supertypes != null
                && contains(supertypes.getValue(Type.NAMES), superName);
    }

    @Override
    public boolean isNodeType(String typeName, String superName) {
        return isa(getTypes(), typeName, superName);
    }

    /**
     * Returns all the node types of the given node, in a breadth-first
     * traversal order of the type hierarchy.
     *
     * @param node node instance
     * @return all types of the given node
     * @throws RepositoryException if the type information can not be accessed
     */
    @Override
    public EffectiveNodeType getEffectiveNodeType(Node node)
            throws RepositoryException {
        NodeTypeImpl primary = (NodeTypeImpl) node.getPrimaryNodeType(); // FIXME
        NodeType[] mixins = node.getMixinNodeTypes();
        NodeTypeImpl[] mixinImpls = new NodeTypeImpl[mixins.length];
        for (int i = 0; i < mixins.length; i++) {
            mixinImpls[i] = (NodeTypeImpl) mixins[i]; // FIXME
        }
        return new EffectiveNodeTypeImpl(primary, mixinImpls, this);
    }

    @Override
    public EffectiveNodeType getEffectiveNodeType(Tree tree) throws RepositoryException {
        NodeTypeImpl primaryType;
        PropertyState jcrPrimaryType = tree.getProperty(JCR_PRIMARYTYPE);
        if (jcrPrimaryType != null) {
            String ntName = jcrPrimaryType.getValue(STRING);
            primaryType = internalGetNodeType(ntName);
        } else {
            throw new RepositoryException("Node at "+tree.getPath()+" has no primary type.");
        }

        PropertyState jcrMixinType = tree.getProperty(JCR_MIXINTYPES);
        if (jcrMixinType == null) {
            return new EffectiveNodeTypeImpl(primaryType, this);
        } else {
            NodeTypeImpl[] mixinTypes = new NodeTypeImpl[jcrMixinType.count()];
            for (int i = 0; i < mixinTypes.length; i++) {
                mixinTypes[i] = internalGetNodeType(jcrMixinType.getValue(Type.NAME, i));
            }
            return new EffectiveNodeTypeImpl(primaryType, mixinTypes, this);
        }
    }

    //-------------------------------------------------< DefinitionProvider >---

    @Nonnull
    @Override
    public NodeDefinition getRootDefinition() throws RepositoryException {
        return new RootNodeDefinition(this);
    }

    @Nonnull
    @Override
    public NodeDefinition getDefinition(@Nonnull Tree parent, @Nonnull String nodeName)
            throws RepositoryException {
        checkNotNull(parent);
        checkNotNull(nodeName);
        EffectiveNodeType effective = getEffectiveNodeType(parent);
        return effective.getNodeDefinition(nodeName, null);
    }

    @Nonnull
    @Override
    public NodeDefinition getDefinition(
            @Nonnull Tree parent, @Nonnull Tree targetNode)
            throws RepositoryException {
        checkNotNull(parent);
        checkNotNull(targetNode);

        String name = dropIndexFromName(targetNode.getName());
        EffectiveNodeType eff = getEffectiveNodeType(parent);
        return eff.getNodeDefinition(name, getEffectiveNodeType(targetNode));
    }

    @Nonnull
    @Override
    public PropertyDefinition getDefinition(
            Tree parent, PropertyState property, boolean exactTypeMatch)
            throws RepositoryException {
        Type<?> type = property.getType();
        EffectiveNodeType effective = getEffectiveNodeType(parent);
        return effective.getPropertyDefinition(
                property.getName(), type.isArray(), type.tag(), exactTypeMatch);
    }

    //-----------------------------------------------------------< internal >---

    NodeTypeImpl internalGetNodeType(String oakName) throws NoSuchNodeTypeException {
        Tree types = getTypes();
        if (types != null) {
            Tree type = types.getChild(oakName);
            if (type.exists()) {
                return new NodeTypeImpl(type, getNamePathMapper());
            }
        }
        throw new NoSuchNodeTypeException(getNamePathMapper().getJcrName(oakName));
    }

}
