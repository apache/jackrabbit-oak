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
import java.security.PrivilegedAction;
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
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeExistsException;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.version.OnParentVersionAction;
import javax.security.auth.Subject;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.commons.cnd.CompactNodeTypeDefReader;
import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.DefaultConflictHandler;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.util.NodeUtil;

import static org.apache.jackrabbit.JcrConstants.JCR_AUTOCREATED;
import static org.apache.jackrabbit.JcrConstants.JCR_CHILDNODEDEFINITION;
import static org.apache.jackrabbit.JcrConstants.JCR_DEFAULTPRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_DEFAULTVALUES;
import static org.apache.jackrabbit.JcrConstants.JCR_HASORDERABLECHILDNODES;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_MANDATORY;
import static org.apache.jackrabbit.JcrConstants.JCR_MULTIPLE;
import static org.apache.jackrabbit.JcrConstants.JCR_NAME;
import static org.apache.jackrabbit.JcrConstants.JCR_NODETYPENAME;
import static org.apache.jackrabbit.JcrConstants.JCR_ONPARENTVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYITEMNAME;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_PROPERTYDEFINITION;
import static org.apache.jackrabbit.JcrConstants.JCR_PROTECTED;
import static org.apache.jackrabbit.JcrConstants.JCR_REQUIREDPRIMARYTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_REQUIREDTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SAMENAMESIBLINGS;
import static org.apache.jackrabbit.JcrConstants.JCR_SUPERTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.JCR_VALUECONSTRAINTS;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.JcrConstants.NT_CHILDNODEDEFINITION;
import static org.apache.jackrabbit.JcrConstants.NT_NODETYPE;
import static org.apache.jackrabbit.JcrConstants.NT_PROPERTYDEFINITION;
import static org.apache.jackrabbit.oak.plugins.type.NodeTypeConstants.JCR_AVAILABLE_QUERY_OPERATORS;
import static org.apache.jackrabbit.oak.plugins.type.NodeTypeConstants.JCR_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.plugins.type.NodeTypeConstants.JCR_IS_FULLTEXT_SEARCHABLE;
import static org.apache.jackrabbit.oak.plugins.type.NodeTypeConstants.JCR_IS_QUERYABLE;
import static org.apache.jackrabbit.oak.plugins.type.NodeTypeConstants.JCR_IS_QUERY_ORDERABLE;
import static org.apache.jackrabbit.oak.plugins.type.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.type.NodeTypeConstants.NODE_TYPES_PATH;

public class NodeTypeManagerImpl extends AbstractNodeTypeManager {

    private final ContentSession session;

    private final NamePathMapper mapper;

    private final ValueFactory factory;

    public NodeTypeManagerImpl(ContentSession session, NamePathMapper mapper, ValueFactory factory) {
        this.session = session;
        this.mapper = mapper;
        this.factory = factory;
    }

    public static void registerBuiltInNodeTypes(ContentSession session) {
        new NodeTypeManagerImpl(session, NamePathMapperImpl.DEFAULT, null).registerBuiltinNodeTypes();
    }

    private void registerBuiltinNodeTypes() {
        // FIXME: migrate custom node types as well.
        // FIXME: registration of built-in node types should be moved to repo-setup
        //        as the jcr:nodetypes tree is protected and the editing session may
        //        not have sufficient permission to register node types or may
        //        even have limited read-permission on the jcr:nodetypes path.
        if (!nodeTypesInContent()) {
            Subject admin = new Subject();
            admin.getPrincipals().add(AdminPrincipal.INSTANCE);
            Subject.doAs(admin, new PrivilegedAction<Void>() {
                @Override
                public Void run() {
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
                    return null;
                }
            });
        }
    }

    @Override
    protected Tree getTypes() {
        return session.getCurrentRoot().getTree(NODE_TYPES_PATH);
    }

    @Override
    protected ValueFactory getValueFactory() {
        return factory;
    }

    @Nonnull
    @Override
    protected NamePathMapper getNamePathMapper() {
        return mapper;
    }

    @Override
    protected CoreValueFactory getCoreValueFactory() {
        return session.getCoreValueFactory();
    }

    @Override
    protected NameMapper getNameMapper() {
        return mapper;
    }

    //----------------------------------------------------< NodeTypeManager >---

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
    public final NodeTypeIterator registerNodeTypes(NodeTypeDefinition[] ntds, boolean allowUpdate)
            throws RepositoryException {
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

        NodeUtil node = new NodeUtil(type, getCoreValueFactory(), mapper);
        node.setName(JCR_PRIMARYTYPE, NT_NODETYPE);
        node.setName(JCR_NODETYPENAME, jcrName);
        node.setNames(JCR_SUPERTYPES, ntd.getDeclaredSupertypeNames());
        node.setBoolean(JCR_IS_ABSTRACT, ntd.isAbstract());
        node.setBoolean(JCR_IS_QUERYABLE, ntd.isQueryable());
        node.setBoolean(JCR_ISMIXIN, ntd.isMixin());

        // TODO fail if not orderable but a supertype is orderable. See 3.7.6.7 Node Type Attribute Subtyping Rules
        node.setBoolean(JCR_HASORDERABLECHILDNODES, ntd.hasOrderableChildNodes());
        String primaryItemName = ntd.getPrimaryItemName();

        // TODO fail if a supertype specifies a different primary item. See 3.7.6.7 Node Type Attribute Subtyping Rules
        if (primaryItemName != null) {
            node.setName(JCR_PRIMARYITEMNAME, primaryItemName);
        }

        // TODO fail on invalid item definitions. See 3.7.6.8 Item Definitions in Subtypes
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

        return new NodeTypeImpl(this, this.factory, this.mapper, node);
    }

    private static void internalRegisterItemDefinition(
            NodeUtil node, ItemDefinition def) {
        String name = def.getName();
        if (!"*".equals(name)) {
            node.setName(JCR_NAME, name);
        }

        // TODO avoid unbounded recursive auto creation. See 3.7.2.3.5 Chained Auto-creation
        node.setBoolean(JCR_AUTOCREATED, def.isAutoCreated());
        node.setBoolean(JCR_MANDATORY, def.isMandatory());
        node.setBoolean(JCR_PROTECTED, def.isProtected());
        node.setString(
                JCR_ONPARENTVERSION,
                OnParentVersionAction.nameFromValue(def.getOnParentVersion()));
    }

    private static void internalRegisterPropertyDefinition(
            NodeUtil node, PropertyDefinition def) {
        internalRegisterItemDefinition(node, def);

        node.setString(
                JCR_REQUIREDTYPE,
                PropertyType.nameFromValue(def.getRequiredType()).toUpperCase());
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

    private static void internalRegisterNodeDefinition(NodeUtil node, NodeDefinition def) {
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
