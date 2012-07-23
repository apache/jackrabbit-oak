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
package org.apache.jackrabbit.oak.jcr.nodetype;

import java.util.List;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.PathNotFoundException;
import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.ItemDefinition;
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
import javax.jcr.version.OnParentVersionAction;

import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.DefaultConflictHandler;
import org.apache.jackrabbit.oak.jcr.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.NodeImpl;
import org.apache.jackrabbit.oak.jcr.SessionDelegate;

import com.google.common.collect.Lists;

public class NodeTypeManagerImpl implements NodeTypeManager {

    private static final String PATH = "/jcr:system/jcr:nodeTypes";

    private final SessionDelegate sd;

    public NodeTypeManagerImpl(SessionDelegate sd) {
        this.sd = sd;
    }

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

    private String getTypePath(String name) {
        // TODO: validate name
        return PATH + '/' + name;
    }

    //---------------------------------------------------< NodeTypeManager >--

    @Override
    public boolean hasNodeType(String name) throws RepositoryException {
        return sd.getSession().nodeExists(getTypePath(name));
    }

    @Override
    public NodeType getNodeType(String name) throws RepositoryException {
        try {
            return new NodeTypeImpl(
                    this, sd.getSession().getNode(getTypePath(name)));
        } catch (PathNotFoundException e) {
            throw new NoSuchNodeTypeException("Type not found: " + name, e);
        }
    }

    @Override
    public NodeTypeIterator getAllNodeTypes() throws RepositoryException {
        List<NodeType> types = Lists.newArrayList();
        Node node = sd.getSession().getNode(PATH);
        NodeIterator iterator = node.getNodes();
        while (iterator.hasNext()) {
            types.add(new NodeTypeImpl(this, iterator.nextNode()));
        }
        return new NodeTypeIteratorAdapter(types);
    }

    @Override
    public NodeTypeIterator getPrimaryNodeTypes() throws RepositoryException {
        List<NodeType> types = Lists.newArrayList();
        Node node = sd.getSession().getNode(PATH);
        NodeIterator iterator = node.getNodes();
        while (iterator.hasNext()) {
            NodeType type = new NodeTypeImpl(this, iterator.nextNode());
            if (!type.isMixin()) {
                types.add(type);
            }
        }
        return new NodeTypeIteratorAdapter(types);
    }

    @Override
    public NodeTypeIterator getMixinNodeTypes() throws RepositoryException {
        List<NodeType> types = Lists.newArrayList();
        Node node = sd.getSession().getNode(PATH);
        NodeIterator iterator = node.getNodes();
        while (iterator.hasNext()) {
            NodeType type = new NodeTypeImpl(this, iterator.nextNode());
            if (type.isMixin()) {
                types.add(type);
            }
        }
        return new NodeTypeIteratorAdapter(types);
    }

    @Override
    public NodeTypeTemplate createNodeTypeTemplate()
            throws RepositoryException {
        return new NodeTypeTemplateImpl(this, sd.getValueFactory());
    }

    @Override
    public NodeTypeTemplate createNodeTypeTemplate(NodeTypeDefinition ntd)
            throws RepositoryException {
        return new NodeTypeTemplateImpl(this, sd.getValueFactory(), ntd);
    }

    @Override
    public NodeDefinitionTemplate createNodeDefinitionTemplate() {
        return new NodeDefinitionTemplateImpl();
    }

    @Override
    public PropertyDefinitionTemplate createPropertyDefinitionTemplate() {
        return new PropertyDefinitionTemplateImpl();
    }

    @Override
    public NodeType registerNodeType(
            NodeTypeDefinition ntd, boolean allowUpdate)
            throws RepositoryException {
        NodeTypeDefinition[] ntds = new NodeTypeDefinition[] { ntd };
        return registerNodeTypes(ntds, allowUpdate).nextNodeType();
    }

    @Override
    public NodeTypeIterator registerNodeTypes(
            NodeTypeDefinition[] ntds, boolean allowUpdate)
            throws RepositoryException {
        Root root = sd.getContentSession().getCurrentRoot();
        Node types = getNodeTypes(root);

        for (NodeTypeDefinition ntd : ntds) {
            internalRegister(types, ntd, allowUpdate);
        }

        commitChanges(root);

        List<NodeType> list = Lists.newArrayList();
        for (NodeTypeDefinition ntd : ntds) {
            list.add(getNodeType(ntd.getName()));
        }
        return new NodeTypeIteratorAdapter(list);
    }

    @Override
    public void unregisterNodeType(String name) throws RepositoryException {
        Root root = sd.getContentSession().getCurrentRoot();
        Node types = getNodeTypes(root);

        try {
            types.getNode(name).remove();
        } catch (PathNotFoundException e) {
            throw new NoSuchNodeTypeException(
                    "Node type " + name + " does not exist", e);
        }

        commitChanges(root);
    }

    @Override
    public void unregisterNodeTypes(String[] names) throws RepositoryException {
        Root root = sd.getContentSession().getCurrentRoot();
        Node types = getNodeTypes(root);

        for (String name : names) {
            try {
                types.getNode(name).remove();
            } catch (PathNotFoundException e) {
                throw new NoSuchNodeTypeException(
                        "Node type " + name + " does not exist", e);
            }
        }

        commitChanges(root);
    }

    //-----------------------------------------------------------< private >--

    private void internalRegister(
            Node types, NodeTypeDefinition ntd, boolean allowUpdate)
            throws RepositoryException {
        String name = ntd.getName();
        if (types.hasNode(name)) {
            types.getNode(name).remove();
        }
        Node node = types.addNode(ntd.getName());

        node.setProperty(Property.JCR_NODE_TYPE_NAME, name, PropertyType.NAME);
        node.setProperty(Property.JCR_SUPERTYPES, ntd.getDeclaredSupertypeNames(), PropertyType.NAME);
        node.setProperty(Property.JCR_IS_ABSTRACT, ntd.isAbstract());
        node.setProperty("jcr:isQueryable", ntd.isQueryable()); // TODO: constant
        node.setProperty(Property.JCR_IS_MIXIN, ntd.isMixin());
        node.setProperty(Property.JCR_HAS_ORDERABLE_CHILD_NODES, ntd.hasOrderableChildNodes());
        String primaryItemName = ntd.getPrimaryItemName();
        if (primaryItemName != null) {
            node.setProperty(Property.JCR_PRIMARY_ITEM_NAME, primaryItemName, PropertyType.NAME);
        }

        int pdn = 0;
        for (PropertyDefinition pd : ntd.getDeclaredPropertyDefinitions()) {
            internalRegisterPropertyDefinition(
                    node.addNode("jcr:propertyDefinition[" + (++pdn) + "]"), pd);
        }

        int ndn = 0;
        for (NodeDefinition nd : ntd.getDeclaredChildNodeDefinitions()) {
            internalRegisterNodeDefinition(
                    node.addNode("jcr:childNodeDefinition[" + (++ndn) +"]"), nd);
        }
    }

    private void internalRegisterItemDefinition(Node node, ItemDefinition def)
            throws RepositoryException {
        String name = def.getName();
        if (!"*".equals(name)) {
            node.setProperty(Property.JCR_NAME, name, PropertyType.NAME);
        }
        node.setProperty(Property.JCR_AUTOCREATED, def.isAutoCreated());
        node.setProperty(Property.JCR_MANDATORY, def.isMandatory());
        node.setProperty(Property.JCR_PROTECTED, def.isProtected());
        node.setProperty(
                Property.JCR_ON_PARENT_VERSION,
                OnParentVersionAction.nameFromValue(def.getOnParentVersion()));
    }

    private void internalRegisterPropertyDefinition(
            Node node, PropertyDefinition def) throws RepositoryException {
        internalRegisterItemDefinition(node, def);

        node.setProperty(
                Property.JCR_REQUIRED_TYPE,
                PropertyType.nameFromValue(def.getRequiredType()));
        node.setProperty(
                Property.JCR_MULTIPLE, def.isMultiple());
        node.setProperty(
                "jcr:isFullTextSearchable", def.isFullTextSearchable());
        node.setProperty(
                "jcr:isQueryOrderable", def.isQueryOrderable());
        node.setProperty(
                "jcr:availableQueryOperators",
                def.getAvailableQueryOperators());

        String[] constraints = def.getValueConstraints();
        if (constraints != null) {
            node.setProperty(Property.JCR_VALUE_CONSTRAINTS, constraints);
        }

        Value[] values = def.getDefaultValues();
        if (values != null) {
            node.setProperty(Property.JCR_DEFAULT_VALUES, values);
        }
    }

    private void internalRegisterNodeDefinition(
            Node node, NodeDefinition def) throws RepositoryException {
        internalRegisterItemDefinition(node, def);

        node.setProperty(
                Property.JCR_SAME_NAME_SIBLINGS, def.allowsSameNameSiblings());
        node.setProperty(
                Property.JCR_REQUIRED_PRIMARY_TYPES,
                def.getRequiredPrimaryTypeNames(), PropertyType.NAME);

        String defaultPrimaryType = def.getDefaultPrimaryTypeName();
        if (defaultPrimaryType != null) {
            node.setProperty(
                    Property.JCR_DEFAULT_PRIMARY_TYPE,
                    defaultPrimaryType, PropertyType.NAME);
        }
    }

    private Node getNodeTypes(Root root) throws RepositoryException {
        Tree types = root.getTree(PATH);
        if (types != null) {
            return new NodeImpl(new NodeDelegate(sd, types));
        } else {
            throw new RepositoryException("Node type registry not found");
        }
    }

    private void commitChanges(Root root) throws RepositoryException {
        try {
            root.commit(DefaultConflictHandler.OURS);
            refresh();
        } catch (CommitFailedException e) {
            throw new RepositoryException(
                    "Failed to modify the node type registry", e);
        }
    }

}
