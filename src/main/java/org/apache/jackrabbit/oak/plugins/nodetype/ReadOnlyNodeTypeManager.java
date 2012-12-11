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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
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
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.jcr.PropertyType.UNDEFINED;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

/**
 * Base implementation of a {@link NodeTypeManager} with support for reading
 * node types from the {@link Tree} returned by {@link #getTypes()}. Methods
 * related to node type modifications throw
 * {@link UnsupportedRepositoryOperationException}.
 */
public abstract class ReadOnlyNodeTypeManager implements NodeTypeManager, EffectiveNodeTypeProvider, DefinitionProvider {

    private static final Logger log = LoggerFactory.getLogger(ReadOnlyNodeTypeManager.class);

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
    protected NameMapper getNameMapper() {
        return NamePathMapperImpl.DEFAULT;
    }

    //--------------------------------------------------------------------------
    /**
     * Return a new instance of {@code ReadOnlyNodeTypeManager} that reads node
     * type information from the tree at {@link NodeTypeConstants#NODE_TYPES_PATH}.
     *
     * @param root The root to read node types from.
     * @return a new instance of {@code ReadOnlyNodeTypeManager}.
     */
    @Nonnull
    public static ReadOnlyNodeTypeManager getInstance(final Root root) {
        return new ReadOnlyNodeTypeManager() {
            @Override
            protected Tree getTypes() {
                return root.getTree(NODE_TYPES_PATH);
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
            for (Tree type : types.getChildren()) {
                list.add(new NodeTypeImpl(this, getValueFactory(),
                        new NodeUtil(type, getNameMapper())));

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

    //------------------------------------------< EffectiveNodeTypeProvider >---
    @Override
    public boolean isNodeType(Tree tree, String nodeTypeName) throws RepositoryException {
        if (!hasNodeType(nodeTypeName)) {
            throw new NoSuchNodeTypeException(nodeTypeName);
        }

        String oakName = getOakName(nodeTypeName);
        NodeUtil node = new NodeUtil(tree);
        String ntName = node.getPrimaryNodeTypeName();
        if (ntName == null) {
            return false;
        } else if (oakName.equals(ntName)
                || internalGetNodeType(ntName).isNodeType(nodeTypeName)) {
            return true;
        }
        String[] mixinNames = node.getStrings(JcrConstants.JCR_MIXINTYPES);
        if (mixinNames != null) {
            for (String mixinName : mixinNames) {
                if (oakName.equals(mixinName) || internalGetNodeType(mixinName).isNodeType(nodeTypeName)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns all the node types of the given node, in a breadth-first
     * traversal order of the type hierarchy.
     *
     * @param node node instance
     * @return all types of the given node
     * @throws RepositoryException if the type information can not be accessed
     * @param node
     * @return
     * @throws RepositoryException
     */
    @Override
    public Iterable<NodeType> getEffectiveNodeTypes(Node node) throws RepositoryException {
        Queue<NodeType> queue = Queues.newArrayDeque();
        queue.add(node.getPrimaryNodeType());
        queue.addAll(Arrays.asList(node.getMixinNodeTypes()));

        return getEffectiveNodeTypes(queue);
    }

    @Override
    public Iterable<NodeType> getEffectiveNodeTypes(Tree tree) throws RepositoryException {
        Queue<NodeType> queue = Queues.newArrayDeque();

        NodeType primaryType;
        PropertyState jcrPrimaryType = tree.getProperty(JCR_PRIMARYTYPE);
        if (jcrPrimaryType != null) {
            String ntName = jcrPrimaryType.getValue(STRING);
            primaryType = getNodeType(ntName);
        } else {
            throw new RepositoryException("Node at "+tree.getPath()+" has no primary type.");
        }
        queue.add(primaryType);

        List<NodeType> mixinTypes = Lists.newArrayList();
        PropertyState jcrMixinType = tree.getProperty(JCR_MIXINTYPES);
        if (jcrMixinType != null) {
            for (String ntName : jcrMixinType.getValue(STRINGS)) {
                mixinTypes.add(getNodeType(ntName));
            }
        }
        queue.addAll(mixinTypes);

        return getEffectiveNodeTypes(queue);
    }

    //-------------------------------------------------< DefinitionProvider >---

    @Override
    public NodeDefinition getRootDefinition() throws RepositoryException {
        return new RootNodeDefinition(this);
    }

    @Nonnull
    @Override
    public NodeDefinition getDefinition(@Nonnull Node parent,
                                        @Nonnull String nodeName)
            throws RepositoryException {
        checkNotNull(parent);
        checkNotNull(nodeName);
        List<NodeDefinition> residualDefs = new ArrayList<NodeDefinition>();
        for (NodeType nt : getEffectiveNodeTypes(parent)) {
            for (NodeDefinition def : nt.getDeclaredChildNodeDefinitions()) {
                String defName = def.getName();
                if (nodeName.equals(defName)
                        && def.getDefaultPrimaryTypeName() != null) {
                    return def;
                } else if ("*".equals(defName)) {
                    residualDefs.add(def);
                }
            }
        }

        for (NodeDefinition def : residualDefs) {
            if (def.getDefaultPrimaryTypeName() != null) {
                // return the first definition with a default primary type
                return def;
            }
        }

        throw new RepositoryException("No matching node definition found for " + nodeName);
    }

    @Override
    public NodeDefinition getDefinition(@Nonnull Node parent, @Nonnull Node targetNode) throws RepositoryException {
        String name = targetNode.getName();
        List<NodeDefinition> residualDefs = new ArrayList<NodeDefinition>();
        // TODO: This may need to be optimized
        for (NodeType nt : getEffectiveNodeTypes(parent)) {
            for (NodeDefinition def : nt.getDeclaredChildNodeDefinitions()) {
                String defName = def.getName();
                if (name.equals(defName)) {
                    boolean match = true;
                    for (String type : def.getRequiredPrimaryTypeNames()) {
                        if (!targetNode.isNodeType(type)) {
                            match = false;
                        }
                    }
                    if (match) {
                        return def;
                    }
                } else if ("*".equals(defName)) {
                    residualDefs.add(def);
                }
            }
        }

        for (NodeDefinition def : residualDefs) {
            String defName = def.getName();
            if ("*".equals(defName)) {
                boolean match = true;
                for (String type : def.getRequiredPrimaryTypeNames()) {
                    if (!targetNode.isNodeType(type)) {
                        match = false;
                    }
                }
                if (match) {
                    return def;
                }
            }
        }

        throw new RepositoryException("No matching node definition found for " + targetNode.getName());
    }

    @Override
    public NodeDefinition getDefinition(Iterable<NodeType> parentNodeTypes, String nodeName, NodeType nodeType)
            throws RepositoryException {

        NodeDefinition def = getDefinitionOrNull(parentNodeTypes, nodeName, nodeType);
        if (def == null) {
            throw new RepositoryException("No matching node definition found for " + nodeName);
        }

        return def;
    }

    /**
     * Same as {@link #getDefinition(Iterable, String, javax.jcr.nodetype.NodeType)} but returns
     * {@code null} if no matching definition could be found instead of throwing a
     * {@code RepositoryException}.
     */
    public NodeDefinition getDefinitionOrNull(Iterable<NodeType> parentNodeTypes, String nodeName, NodeType nodeType) {
        List<NodeDefinition> residualDefs = new ArrayList<NodeDefinition>();
        // TODO: This may need to be optimized
        // TODO: cleanup redundancy with getDefinition(Node, Node)
        for (NodeType nt : parentNodeTypes) {
            for (NodeDefinition def : nt.getDeclaredChildNodeDefinitions()) {
                String defName = def.getName();
                if (nodeName.equals(defName)) {
                    boolean match = true;
                    // TODO: check again if passing null nodeType is legal.
                    if (nodeType != null) {
                        for (String type : def.getRequiredPrimaryTypeNames()) {
                            if (!nodeType.isNodeType(type)) {
                                match = false;
                            }
                        }
                    }
                    if (match) {
                        return def;
                    }
                } else if ("*".equals(defName)) {
                    residualDefs.add(def);
                }
            }
        }

        for (NodeDefinition def : residualDefs) {
            String defName = def.getName();
            if ("*".equals(defName)) {
                boolean match = true;
                for (String type : def.getRequiredPrimaryTypeNames()) {
                    if (!nodeType.isNodeType(type)) {
                        match = false;
                    }
                }
                if (match) {
                    return def;
                }
            }
        }

        return null;
    }

    @Override
    public PropertyDefinition getDefinition(Node parent, Property targetProperty) throws RepositoryException {
        String name = targetProperty.getName();
        boolean isMultiple = targetProperty.isMultiple();
        int type = UNDEFINED;
        if (isMultiple) {
            Value[] values = targetProperty.getValues();
            if (values.length > 0) {
                type = values[0].getType();
            }
        } else {
            type = targetProperty.getValue().getType();
        }

        // TODO: This may need to be optimized
        List<PropertyDefinition> residualDefs = new ArrayList<PropertyDefinition>();
        for (NodeType nt : getEffectiveNodeTypes(parent)) {
            for (PropertyDefinition def : nt.getDeclaredPropertyDefinitions()) {
                String defName = def.getName();
                int defType = def.getRequiredType();
                if ((name.equals(defName))
                        && (type == defType || UNDEFINED == type || UNDEFINED == defType)
                        && isMultiple == def.isMultiple()) {
                    return def;
                } else if ("*".equals(defName)) {
                    residualDefs.add(def);
                }
            }
        }

        for (PropertyDefinition def : residualDefs) {
            String defName = def.getName();
            int defType = def.getRequiredType();
            if (("*".equals(defName))
                    && (type == defType || UNDEFINED == type || UNDEFINED == defType)
                    && isMultiple == def.isMultiple()) {
                return def;
            }
        }

        // FIXME: Shouldn't be needed
        for (NodeType nt : getEffectiveNodeTypes(parent)) {
            for (PropertyDefinition def : nt.getDeclaredPropertyDefinitions()) {
                String defName = def.getName();
                if ((name.equals(defName) || "*".equals(defName))
                        && type == PropertyType.STRING
                        && isMultiple == def.isMultiple()) {
                    return def;
                }
            }
        }

        throw new RepositoryException("No matching property definition found for " + targetProperty.getName());
    }

    @Nonnull
    @Override
    public PropertyDefinition getDefinition(Tree parent, PropertyState propertyState) throws RepositoryException {
        return getDefinition(parent, propertyState.getName(), propertyState.isArray(), propertyState.getType().tag(), true);
    }

    @Nonnull
    @Override
    public PropertyDefinition getDefinition(Node parent, String propertyName, boolean isMultiple, int type, boolean exactTypeMatch) throws RepositoryException {
        return getPropertyDefinition(getEffectiveNodeTypes(parent), propertyName, isMultiple, type, exactTypeMatch);
    }

    @Nonnull
    @Override
    public PropertyDefinition getDefinition(Tree parent, String propertyName, boolean isMultiple, int type, boolean exactTypeMatch) throws RepositoryException {
        return getPropertyDefinition(getEffectiveNodeTypes(parent), propertyName, isMultiple, type, exactTypeMatch);
    }

    @Nonnull
    @Override
    public PropertyDefinition getDefinition(Iterable<NodeType> nodeTypes, String propertyName, boolean isMultiple,
            int type, boolean exactTypeMatch) throws RepositoryException {
        Queue<NodeType> queue = Queues.newArrayDeque(nodeTypes);
        Collection<NodeType> effective = getEffectiveNodeTypes(queue);
        return getPropertyDefinition(effective, propertyName, isMultiple, type, exactTypeMatch);
    }

    //------------------------------------------------------------< private >---
    private NodeType internalGetNodeType(String oakName) throws NoSuchNodeTypeException {
        Tree types = getTypes();
        if (types != null) {
            Tree type = types.getChild(oakName);
            if (type != null) {
                return new NodeTypeImpl(this, getValueFactory(), new NodeUtil(type, getNameMapper()));
            }
        }
        throw new NoSuchNodeTypeException(getNameMapper().getJcrName(oakName));
    }

    private static Collection<NodeType> getEffectiveNodeTypes(Queue<NodeType> queue) {
        Map<String, NodeType> types = Maps.newHashMap();
        while (!queue.isEmpty()) {
            NodeType type = queue.remove();
            String name = type.getName();
            if (!types.containsKey(name)) {
                types.put(name, type);
                queue.addAll(Arrays.asList(type.getDeclaredSupertypes()));
            }
        }

        return types.values();
    }

    private static PropertyDefinition getPropertyDefinition(Iterable<NodeType> effectiveNodeTypes,
            String propertyName, boolean isMultiple,
            int type, boolean exactTypeMatch) throws RepositoryException {
        // TODO: This may need to be optimized
        for (NodeType nt : effectiveNodeTypes) {
            for (PropertyDefinition def : nt.getDeclaredPropertyDefinitions()) {
                String defName = def.getName();
                int defType = def.getRequiredType();
                if (propertyName.equals(defName)
                        && isMultiple == def.isMultiple()
                        &&(!exactTypeMatch || (type == defType || UNDEFINED == type || UNDEFINED == defType))) {
                    return def;
                }
            }
        }

        // try if there is a residual definition
        for (NodeType nt : effectiveNodeTypes) {
            for (PropertyDefinition def : nt.getDeclaredPropertyDefinitions()) {
                String defName = def.getName();
                int defType = def.getRequiredType();
                if ("*".equals(defName)
                        && isMultiple == def.isMultiple()
                        && (!exactTypeMatch || (type == defType || UNDEFINED == type || UNDEFINED == defType))) {
                    return def;
                }
            }
        }

        // FIXME: Shouldn't be needed
        for (NodeType nt : effectiveNodeTypes) {
            for (PropertyDefinition def : nt.getDeclaredPropertyDefinitions()) {
                String defName = def.getName();
                if ((propertyName.equals(defName) || "*".equals(defName))
                        && type == PropertyType.STRING
                        && isMultiple == def.isMultiple()) {
                    return def;
                }
            }
        }
        throw new RepositoryException("No matching property definition found for " + propertyName);
    }
}
