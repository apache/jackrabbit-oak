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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinitionTemplate;

import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.jcr.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.namepath.NameMapper;

public class NodeTypeManagerImpl implements NodeTypeManager {

    private final ValueFactoryImpl vf;
    private final NameMapper mapper;
    private final NodeTypeManagerDelegate ntmd;
    private final Map<String, NodeType> typemap = new HashMap<String, NodeType>();

    public NodeTypeManagerImpl(SessionDelegate sd) throws RepositoryException {
        this.vf = sd.getValueFactory();
        this.mapper = sd.getNamePathMapper();
        this.ntmd = new NodeTypeManagerDelegate(sd.getContentSession(), sd.getValueFactory().getCoreValueFactory());
    }

    private void init() {
        if (typemap.isEmpty()) {
            List<NodeTypeDelegate> alltypes = ntmd.getAllNodeTypeDelegates();

            for (NodeTypeDelegate t : alltypes) {
                NodeType nt = new NodeTypeImpl(this, vf, mapper, t);
                typemap.put(t.getName(), nt);
            }
        }
    }

    @Override
    public boolean hasNodeType(String name) throws RepositoryException {
        init();
        String oakName = mapper.getOakName(name); // can be null, which is fine
        return typemap.containsKey(oakName);
    }

    @Override
    public NodeType getNodeType(String name) throws RepositoryException {
        init();
        String oakName = mapper.getOakName(name); // can be null, which is fine
        NodeType type = typemap.get(oakName);
        if (type == null) {
            throw new NoSuchNodeTypeException("Unknown node type: " + name);
        }
        return type;
    }

    @Override
    public NodeTypeIterator getAllNodeTypes() throws RepositoryException {
        init();
        return new NodeTypeIteratorAdapter(typemap.values());
    }

    @Override
    public NodeTypeIterator getPrimaryNodeTypes() throws RepositoryException {
        init();
        Collection<NodeType> primary = new ArrayList<NodeType>();
        for (NodeType type : typemap.values()) {
            if (!type.isMixin()) {
                primary.add(type);
            }
        }
        return new NodeTypeIteratorAdapter(primary);
    }

    @Override
    public NodeTypeIterator getMixinNodeTypes() throws RepositoryException {
        init();
        Collection<NodeType> mixin = new ArrayList<NodeType>();
        for (NodeType type : typemap.values()) {
            if (type.isMixin()) {
                mixin.add(type);
            }
        }
        return new NodeTypeIteratorAdapter(mixin);
    }

    @Override
    public NodeTypeTemplate createNodeTypeTemplate() throws RepositoryException {
        return new NodeTypeTemplateImpl();
    }

    @Override
    public NodeTypeTemplate createNodeTypeTemplate(NodeTypeDefinition ntd) throws RepositoryException {
        return new NodeTypeTemplateImpl(ntd);
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
        NodeTypeDelegate delegate = new NodeTypeDelegate(
                ntd.getName(),
                ntd.getDeclaredSupertypeNames(), ntd.getPrimaryItemName(),
                ntd.isMixin(), ntd.isAbstract(), ntd.hasOrderableChildNodes());
        NodeType type = new NodeTypeImpl(this, vf, mapper, delegate);
        typemap.put(ntd.getName(), type);
        return type;
    }

    @Override
    public NodeTypeIterator registerNodeTypes(NodeTypeDefinition[] ntds, boolean allowUpdate) throws RepositoryException {
        // TODO handle inter-type dependencies (OAK-66)
        NodeType[] types = new NodeType[ntds.length];
        for (int i = 0; i < ntds.length; i++) {
            types[i] = registerNodeType(ntds[i], allowUpdate);
        }
        return new NodeTypeIteratorAdapter(Arrays.asList(types));
    }

    @Override
    public void unregisterNodeType(String name) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public void unregisterNodeTypes(String[] names) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }
}
