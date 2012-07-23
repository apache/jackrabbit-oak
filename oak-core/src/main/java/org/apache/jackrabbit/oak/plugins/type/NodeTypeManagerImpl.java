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

public class NodeTypeManagerImpl implements NodeTypeManager {

    private final ContentSession session;

    private final NameMapper mapper;

    private final ValueFactory factory;

    public NodeTypeManagerImpl(
            ContentSession session, NameMapper mapper, ValueFactory factory) {
        this.session = session;
        this.mapper = mapper;
        this.factory = factory;

        if (session.getCurrentRoot().getTree("/jcr:system/jcr:nodeTypes") == null) {
            try {
                InputStream stream = NodeTypeManagerImpl.class.getResourceAsStream(
                        "builtin_nodetypes.cnd");
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
                                && !"nt:base".equals(template.getName())) {
                            String[] supertypes =
                                    template.getDeclaredSupertypeNames();
                            if (supertypes.length == 0) {
                                template.setDeclaredSuperTypeNames(
                                        new String[] { "nt:base" });
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
                                    withBase[0] = "nt:base";
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

    protected String getOakName(String name) throws RepositoryException {
        return name; // TODO
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

    //---------------------------------------------------< NodeTypeManager >--

    @Override
    public boolean hasNodeType(String name) throws RepositoryException {
        Tree types = session.getCurrentRoot().getTree(
                "/jcr:system/jcr:nodeTypes");
        return types != null && types.hasChild(mapper.getOakName(name));
    }

    @Override
    public NodeType getNodeType(String name) throws RepositoryException {
        Tree types = session.getCurrentRoot().getTree(
                "/jcr:system/jcr:nodeTypes");
        if (types != null) {
            Tree type = types.getChild(mapper.getOakName(name));
            if (type != null) {
                return new NodeTypeImpl(this, factory, new NodeUtil(
                        session.getCoreValueFactory(), mapper, type));
            }
        }
        throw new NoSuchNodeTypeException(name);
    }

    @Override
    public NodeTypeIterator getAllNodeTypes() throws RepositoryException {
        List<NodeType> list = Lists.newArrayList();
        Tree types = session.getCurrentRoot().getTree(
                "/jcr:system/jcr:nodeTypes");
        if (types != null) {
            for (Tree type : types.getChildren()) {
                list.add(new NodeTypeImpl(this, factory, new NodeUtil(
                        session.getCoreValueFactory(), mapper, type)));
                
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
            for (int i = 0; i < ntds.length; i++) {
                list.add(internalRegister(types, ntds[i], allowUpdate));
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
        String oakName = mapper.getOakName(jcrName);

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
        NodeUtil node = new NodeUtil(factory, mapper, type);
        node.setName("jcr:nodeTypeName", jcrName);
        node.setNames("jcr:supertypes", ntd.getDeclaredSupertypeNames());
        node.setBoolean("jcr:isAbstract", ntd.isAbstract());
        node.setBoolean("jcr:isQueryable", ntd.isQueryable());
        node.setBoolean("jcr:isMixin", ntd.isMixin());
        node.setBoolean("jcr:hasOrderableChildNodes", ntd.hasOrderableChildNodes());
        String primaryItemName = ntd.getPrimaryItemName();
        if (primaryItemName != null) {
            node.setName("jcr:primaryItemName", primaryItemName);
        }

        int pdn = 1;
        for (PropertyDefinition pd : ntd.getDeclaredPropertyDefinitions()) {
            Tree def = type.addChild("jcr:propertyDefinition" + pdn++);
            internalRegisterPropertyDefinition(
                    new NodeUtil(factory, mapper, def), pd);
        }

        int ndn = 1;
        for (NodeDefinition nd : ntd.getDeclaredChildNodeDefinitions()) {
            Tree def = type.addChild("jcr:childNodeDefinition" + ndn++);
            internalRegisterNodeDefinition(
                    new NodeUtil(factory, mapper, def), nd);
        }

        return new NodeTypeImpl(this, this.factory, node);
    }

    private void internalRegisterItemDefinition(
            NodeUtil node, ItemDefinition def) {
        String name = def.getName();
        if (!"*".equals(name)) {
            node.setName("jcr:name", name);
        }
        node.setBoolean("jcr:autoCreated", def.isAutoCreated());
        node.setBoolean("jcr:mandatory", def.isMandatory());
        node.setBoolean("jcr:protected", def.isProtected());
        node.setString(
                "jcr:onParentVersion",
                OnParentVersionAction.nameFromValue(def.getOnParentVersion()));
    }

    private void internalRegisterPropertyDefinition(
            NodeUtil node, PropertyDefinition def)
            throws RepositoryException {
        internalRegisterItemDefinition(node, def);

        node.setString(
                "jcr:requiredType",
                PropertyType.nameFromValue(def.getRequiredType()));
        node.setBoolean("jcr:multiple", def.isMultiple());
        node.setBoolean("jcr:isFullTextSearchable", def.isFullTextSearchable());
        node.setBoolean("jcr:isQueryOrderable", def.isQueryOrderable());
        node.setStrings("jcr:availableQueryOperators", def.getAvailableQueryOperators());

        String[] constraints = def.getValueConstraints();
        if (constraints != null) {
            node.setStrings("jcr:valueConstraints", constraints);
        }

        Value[] values = def.getDefaultValues();
        if (values != null) {
            node.setValues("jcr:defaultValues", values);
        }
    }

    private void internalRegisterNodeDefinition(
            NodeUtil node, NodeDefinition def) {
        internalRegisterItemDefinition(node, def);

        node.setBoolean("jcr:sameNameSiblings", def.allowsSameNameSiblings());
        node.setNames(
                "jcr:requiredPrimaryTypes",
                def.getRequiredPrimaryTypeNames());
        String defaultPrimaryType = def.getDefaultPrimaryTypeName();
        if (defaultPrimaryType != null) {
            node.setName("jcr:defaultPrimaryType", defaultPrimaryType);
        }
    }

    private Tree getOrCreateNodeTypes(Root root) {
        Tree types = root.getTree("/jcr:system/jcr:nodeTypes");
        if (types == null) {
            Tree system = root.getTree("/jcr:system");
            if (system == null) {
                system = root.getTree("/").addChild("jcr:system");
            }
            types = system.addChild("jcr:nodeTypes");
        }
        return types;
    }

    @Override
    public void unregisterNodeType(String name) throws RepositoryException {
        Tree type = null;
        Root root = session.getCurrentRoot();
        Tree types = root.getTree("/jcr:system/jcr:nodeTypes");
        if (types != null) {
            type = types.getChild(mapper.getOakName(name));
        }
        if (type == null) {
            // TODO: Degrade gracefully? Or throw NoSuchNodeTypeException?
            throw new RepositoryException(
                    "Node type " + name + " can not be unregistered");
        }

        try {
            type.remove();
            root.commit(DefaultConflictHandler.OURS);
            refresh();
        } catch (CommitFailedException e) {
            throw new RepositoryException(
                    "Failed to unregister node type " + name, e);
        }
    }

    @Override
    public void unregisterNodeTypes(String[] names) throws RepositoryException {
        Root root = session.getCurrentRoot();
        Tree types = root.getTree("/jcr:system/jcr:nodeTypes");
        if (types == null) {
            // TODO: Degrade gracefully? Or throw NoSuchNodeTypeException?
            throw new RepositoryException("Node types can not be unregistered");
        }

        try {
            for (String name : names) {
                Tree type = types.getChild(mapper.getOakName(name));
                if (type == null) {
                    // TODO: Degrade gracefully? Or throw NoSuchNodeTypeException?
                    throw new RepositoryException(
                            "Node type " + name + " can not be unregistered");
                }
                type.remove();
            }
            root.commit(DefaultConflictHandler.OURS);
            refresh();
        } catch (CommitFailedException e) {
            throw new RepositoryException(
                    "Failed to unregister node types", e);
        }
    }

}
