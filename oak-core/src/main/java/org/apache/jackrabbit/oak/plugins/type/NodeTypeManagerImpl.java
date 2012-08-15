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
package org.apache.jackrabbit.oak.plugins.type;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.ItemDefinition;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeExistsException;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.nodetype.PropertyDefinitionTemplate;
import javax.jcr.version.OnParentVersionAction;

import org.apache.jackrabbit.commons.cnd.CompactNodeTypeDefReader;
import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.DefaultConflictHandler;
import org.apache.jackrabbit.oak.namepath.NameMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.util.NodeUtil;

public class NodeTypeManagerImpl implements NodeTypeManager, NodeTypeConstants {

    private final ContentSession session;

    private final NameMapper mapper;

    private final ValueFactory factory;

    public NodeTypeManagerImpl(
            ContentSession session, NameMapper mapper, ValueFactory factory) {
        this.session = session;
        this.mapper = mapper;
        this.factory = factory;

        // FIXME: migrate custom node types as well.
        // FIXME: registration of built-in node types should be moved to repo-setup
        //        as the jcr:nodetypes tree is protected and the editing session may
        //        not have sufficient permission to register node types or may
        //        even have limited read-permission on the jcr:nodetypes path.
        if (!nodeTypesInContent()) {
            try {
                InputStream stream = NodeTypeManagerImpl.class.getResourceAsStream("builtin_nodetypes.cnd");
                try {
                    CompactNodeTypeDefReader<NodeTypeTemplate, Map<String, String>> reader =
                            new CompactNodeTypeDefReader<NodeTypeTemplate, Map<String, String>>(
                                    new InputStreamReader(stream, "UTF-8"), null, new DefBuilderFactory());
                    Map<String, NodeTypeTemplate> templates = Maps.newHashMap();
                    for (NodeTypeTemplate template : reader.getNodeTypeDefinitions()) {
                        templates.put(template.getName(), template);
                    }
                    for (NodeTypeTemplate template : templates.values()) {
                        if (!template.isMixin()
                                && !NT_BASE.equals(template.getName())) {
                            String[] supertypes =
                                    template.getDeclaredSupertypeNames();
                            if (supertypes.length == 0) {
                                template.setDeclaredSuperTypeNames(
                                        new String[] {NT_BASE});
                            } else {
                                // Check whether we need to add the implicit "nt:base" supertype
                                boolean needsNtBase = true;
                                for (String name : supertypes) {
                                    if (!templates.get(name).isMixin()) {
                                        needsNtBase = false;
                                    }
                                }
                                if (needsNtBase) {
                                    String[] withBase = new String[supertypes.length + 1];
                                    withBase[0] = NT_BASE;
                                    System.arraycopy(supertypes, 0, withBase, 1, supertypes.length);
                                    template.setDeclaredSuperTypeNames(withBase);
                                }
                            }
                        }
                    }
                    registerNodeTypes(templates.values().toArray(
                            new NodeTypeTemplate[templates.size()]), true);
                } finally {
                    stream.close();
                }
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Unable to load built-in node types", e);
            }
        }
    }

    /**
     * Returns the internal name for the specified JCR name.
     *
     * @param jcrName JCR node type name.
     * @return the internal representation of the given JCR name.
     * @throws RepositoryException If there is no valid internal representation
     * of the specified JCR name.
     */
    @Nonnull
    protected String getOakName(String jcrName) throws RepositoryException {
        String oakName = mapper.getOakName(jcrName);
        if (oakName == null) {
            throw new RepositoryException("Invalid JCR name " + jcrName);
        }
        return oakName;
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

    //----------------------------------------------------< NodeTypeManager >---

    @Override
    public boolean hasNodeType(String name) throws RepositoryException {
        Tree types = session.getCurrentRoot().getTree(NODE_TYPES_PATH);
        return types != null && types.hasChild(getOakName(name));
    }

    @Override
    public NodeType getNodeType(String name) throws RepositoryException {
        Tree types = session.getCurrentRoot().getTree(NODE_TYPES_PATH);
        if (types != null) {
            Tree type = types.getChild(getOakName(name));
            if (type != null) {
                return new NodeTypeImpl(this, factory, new NodeUtil(
                        type, session.getCoreValueFactory(), mapper));
            }
        }
        throw new NoSuchNodeTypeException(name);
    }

    @Override
    public NodeTypeIterator getAllNodeTypes() throws RepositoryException {
        List<NodeType> list = Lists.newArrayList();
        Tree types = session.getCurrentRoot().getTree(NODE_TYPES_PATH);
        if (types != null) {
            for (Tree type : types.getChildren()) {
                list.add(new NodeTypeImpl(this, factory, new NodeUtil(
                        type, session.getCoreValueFactory(), mapper)));

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
        return new NodeTypeTemplateImpl(this, factory);
    }

    @Override
    public NodeTypeTemplate createNodeTypeTemplate(NodeTypeDefinition ntd) throws RepositoryException {
        return new NodeTypeTemplateImpl(this, factory, ntd);
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
    public NodeType registerNodeType(NodeTypeDefinition ntd, boolean allowUpdate) throws RepositoryException {
        // TODO proper node type registration... (OAK-66)
        Root root = session.getCurrentRoot();
        Tree types = getOrCreateNodeTypes(root);
        try {
            NodeType type = internalRegister(types, ntd, allowUpdate);
            root.commit(DefaultConflictHandler.OURS);
            return type;
        } catch (CommitFailedException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    public NodeTypeIterator registerNodeTypes(NodeTypeDefinition[] ntds, boolean allowUpdate) throws RepositoryException {
        // TODO handle inter-type dependencies (OAK-66)
        Root root = session.getCurrentRoot();
        Tree types = getOrCreateNodeTypes(root);
        try {
            List<NodeType> list = Lists.newArrayList();
            for (NodeTypeDefinition ntd : ntds) {
                list.add(internalRegister(types, ntd, allowUpdate));
            }
            root.commit(DefaultConflictHandler.OURS);
            return new NodeTypeIteratorAdapter(list);
        } catch (CommitFailedException e) {
            throw new RepositoryException(e);
        }
    }

    private NodeType internalRegister(
            Tree types, NodeTypeDefinition ntd, boolean allowUpdate)
            throws RepositoryException {
        String jcrName = ntd.getName();
        String oakName = getOakName(jcrName);

        Tree type = types.getChild(oakName);
        if (type != null) {
            if (allowUpdate) {
                type.remove();
            } else {
                throw new NodeTypeExistsException(
                        "Node type " + jcrName + " already exists");
            }
        }
        type = types.addChild(oakName);

        CoreValueFactory factory = session.getCoreValueFactory();
        NodeUtil node = new NodeUtil(type, factory, mapper);
        node.setName(JCR_PRIMARYTYPE, NT_NODETYPE);
        node.setName(JCR_NODETYPENAME, jcrName);
        node.setNames(JCR_SUPERTYPES, ntd.getDeclaredSupertypeNames());
        node.setBoolean(JCR_IS_ABSTRACT, ntd.isAbstract());
        node.setBoolean(JCR_IS_QUERYABLE, ntd.isQueryable());
        node.setBoolean(JCR_ISMIXIN, ntd.isMixin());
        node.setBoolean(JCR_HASORDERABLECHILDNODES, ntd.hasOrderableChildNodes());
        String primaryItemName = ntd.getPrimaryItemName();
        if (primaryItemName != null) {
            node.setName(JCR_PRIMARYITEMNAME, primaryItemName);
        }

        int pdn = 1;
        for (PropertyDefinition pd : ntd.getDeclaredPropertyDefinitions()) {
            NodeUtil def = node.addChild(JCR_PROPERTYDEFINITION + pdn++, NT_PROPERTYDEFINITION);
            internalRegisterPropertyDefinition(def, pd);
        }

        int ndn = 1;
        for (NodeDefinition nd : ntd.getDeclaredChildNodeDefinitions()) {
            NodeUtil def = node.addChild(JCR_CHILDNODEDEFINITION + ndn++, NT_CHILDNODEDEFINITION);
            internalRegisterNodeDefinition(def, nd);
        }

        return new NodeTypeImpl(this, this.factory, node);
    }

    private void internalRegisterItemDefinition(
            NodeUtil node, ItemDefinition def) {
        String name = def.getName();
        if (!"*".equals(name)) {
            node.setName(JCR_NAME, name);
        }
        node.setBoolean(JCR_AUTOCREATED, def.isAutoCreated());
        node.setBoolean(JCR_MANDATORY, def.isMandatory());
        node.setBoolean(JCR_PROTECTED, def.isProtected());
        node.setString(
                JCR_ONPARENTVERSION,
                OnParentVersionAction.nameFromValue(def.getOnParentVersion()));
    }

    private void internalRegisterPropertyDefinition(
            NodeUtil node, PropertyDefinition def) {
        internalRegisterItemDefinition(node, def);

        node.setString(
                JCR_REQUIREDTYPE,
                PropertyType.nameFromValue(def.getRequiredType()));
        node.setBoolean(JCR_MULTIPLE, def.isMultiple());
        node.setBoolean(JCR_IS_FULLTEXT_SEARCHABLE, def.isFullTextSearchable());
        node.setBoolean(JCR_IS_QUERY_ORDERABLE, def.isQueryOrderable());
        node.setStrings(JCR_AVAILABLE_QUERY_OPERATORS, def.getAvailableQueryOperators());

        String[] constraints = def.getValueConstraints();
        if (constraints != null) {
            node.setStrings(JCR_VALUECONSTRAINTS, constraints);
        }

        Value[] values = def.getDefaultValues();
        if (values != null) {
            node.setValues(JCR_DEFAULTVALUES, values);
        }
    }

    private void internalRegisterNodeDefinition(NodeUtil node, NodeDefinition def) {
        internalRegisterItemDefinition(node, def);

        node.setBoolean(JCR_SAMENAMESIBLINGS, def.allowsSameNameSiblings());
        node.setNames(
                JCR_REQUIREDPRIMARYTYPES,
                def.getRequiredPrimaryTypeNames());
        String defaultPrimaryType = def.getDefaultPrimaryTypeName();
        if (defaultPrimaryType != null) {
            node.setName(JCR_DEFAULTPRIMARYTYPE, defaultPrimaryType);
        }
    }

    private Tree getOrCreateNodeTypes(Root root) {
        Tree types = root.getTree(NODE_TYPES_PATH);
        if (types == null) {
            Tree system = root.getTree(JCR_SYSTEM);
            if (system == null) {
                system = root.getTree("").addChild(JCR_SYSTEM);
            }
            types = system.addChild(JCR_NODE_TYPES);
        }
        return types;
    }

    private boolean nodeTypesInContent() {
        Root currentRoot = session.getCurrentRoot();
        Tree types = currentRoot.getTree(NODE_TYPES_PATH);
        return types != null && types.getChildrenCount() > 0;
    }

    @Override
    public void unregisterNodeType(String name) throws RepositoryException {
        Tree type = null;
        Root root = session.getCurrentRoot();
        Tree types = root.getTree(NODE_TYPES_PATH);
        if (types != null) {
            type = types.getChild(getOakName(name));
        }
        if (type == null) {
            throw new NoSuchNodeTypeException("Node type " + name + " can not be unregistered.");
        }

        try {
            type.remove();
            root.commit(DefaultConflictHandler.OURS);
            refresh();
        } catch (CommitFailedException e) {
            throw new RepositoryException("Failed to unregister node type " + name, e);
        }
    }

    @Override
    public void unregisterNodeTypes(String[] names) throws RepositoryException {
        Root root = session.getCurrentRoot();
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
            root.commit(DefaultConflictHandler.OURS);
            refresh();
        } catch (CommitFailedException e) {
            throw new RepositoryException("Failed to unregister node types", e);
        }
    }

}
