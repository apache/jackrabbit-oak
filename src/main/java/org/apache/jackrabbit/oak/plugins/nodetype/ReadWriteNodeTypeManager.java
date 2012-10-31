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

import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.commons.cnd.CompactNodeTypeDefReader;
import org.apache.jackrabbit.commons.cnd.ParseException;
import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
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
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_AVAILABLE_QUERY_OPERATORS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_FULLTEXT_SEARCHABLE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_QUERYABLE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_QUERY_ORDERABLE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

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
 *     <li>{@link #getNameMapper()}</li>
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

    /**
     * Utility method for registering node types from a CND format.
     * @param cnd  reader for the CND
     * @throws ParseException  if parsing the CND fails
     * @throws RepositoryException  if registering the node types fails
     */
    public void registerNodeTypes(Reader cnd) throws ParseException, RepositoryException {
        Root root = getWriteRoot();

        CompactNodeTypeDefReader<NodeTypeTemplate, Map<String, String>> reader =
                new CompactNodeTypeDefReader<NodeTypeTemplate, Map<String, String>>(
                        cnd, null, new DefBuilderFactory(root.getTree("/")));

        Map<String, NodeTypeTemplate> templates = Maps.newHashMap();
        for (NodeTypeTemplate template : reader.getNodeTypeDefinitions()) {
            templates.put(template.getName(), template);
        }

        for (NodeTypeTemplate template : templates.values()) {
            if (!template.isMixin() && !NT_BASE.equals(template.getName())) {
                String[] supertypes =
                        template.getDeclaredSupertypeNames();
                if (supertypes.length == 0) {
                    template.setDeclaredSuperTypeNames(
                            new String[] {NT_BASE});
                } else {
                    // Check whether we need to add the implicit "nt:base" supertype
                    boolean needsNtBase = true;
                    for (String name : supertypes) {
                        NodeTypeDefinition st = templates.get(name);
                        if (st == null) {
                            st = getNodeType(name);
                        }
                        if (st != null && !st.isMixin()) {
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

        try {
            internalRegister(root, templates.values(), true);
            root.commit();
            refresh();
        } catch (CommitFailedException e) {
            throw new RepositoryException(e);
        }
    }

    //----------------------------------------------------< NodeTypeManager >---

    @Override
    public NodeType registerNodeType(NodeTypeDefinition ntd, boolean allowUpdate) throws RepositoryException {
        // TODO proper node type registration... (OAK-66)
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
        // TODO handle inter-type dependencies (OAK-66)
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

        NodeUtil node = new NodeUtil(type, getNameMapper());
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
        PropertyDefinition[] propertyDefinitions = ntd.getDeclaredPropertyDefinitions();
        if (propertyDefinitions != null) {
            int pdn = 1;
            for (PropertyDefinition pd : propertyDefinitions) {
                NodeUtil def = node.addChild(JCR_PROPERTYDEFINITION + pdn++, NT_PROPERTYDEFINITION);
                internalRegisterPropertyDefinition(def, pd);
            }
        }

        NodeDefinition[] nodeDefinitions = ntd.getDeclaredChildNodeDefinitions();
        if (nodeDefinitions != null) {
            int ndn = 1;
            for (NodeDefinition nd : nodeDefinitions) {
                NodeUtil def = node.addChild(JCR_CHILDNODEDEFINITION + ndn++, NT_CHILDNODEDEFINITION);
                internalRegisterNodeDefinition(def, nd);
            }
        }

        return new NodeTypeImpl(this, getValueFactory(), node);
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
