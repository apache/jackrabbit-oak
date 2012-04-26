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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.nodetype.InvalidNodeTypeDefinitionException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeExistsException;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinitionTemplate;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.namepath.NameMapper;

public class NodeTypeManagerImpl implements NodeTypeManager {

    private static final Pattern CND_PATTERN =
            Pattern.compile("\\[(\\S*)?\\](.*?)\n\n", Pattern.DOTALL);

    private final NameMapper mapper;

    private final Map<String, NodeType> types = new HashMap<String, NodeType>();

    public NodeTypeManagerImpl(NameMapper mapper) throws RepositoryException {
        this.mapper = mapper;

        try {
            InputStream stream = NodeTypeManagerImpl.class.getResourceAsStream(
                    "builtin_nodetypes.cnd");
            try {
                String cnd = IOUtils.toString(stream, "UTF-8");
                Matcher matcher = CND_PATTERN.matcher(cnd);
                while (matcher.find()) {
                    String name = matcher.group(1);
                    types.put(name, new NodeTypeImpl(
                            this, mapper, name, matcher.group(2)));
                }
            } finally {
                stream.close();
            }
        } catch (IOException e) {
            throw new RepositoryException(
                    "Failed to load built-in node types", e);
        }
    }

    @Override
    public boolean hasNodeType(String name) throws RepositoryException {
        return types.containsKey(mapper.getOakName(name));
    }

    @Override
    public NodeType getNodeType(String name)
            throws NoSuchNodeTypeException, RepositoryException {
        NodeType type = types.get(mapper.getOakName(name));
        if (type == null) {
            throw new NoSuchNodeTypeException("Unknown node type: " + name);
        }
        return type;
    }

    @Override
    public NodeTypeIterator getAllNodeTypes() throws RepositoryException {
        return new NodeTypeIteratorAdapter(types.values());
    }

    @Override
    public NodeTypeIterator getPrimaryNodeTypes() throws RepositoryException {
        Collection<NodeType> primary = new ArrayList<NodeType>();
        for (NodeType type : types.values()) {
            if (!type.isMixin()) {
                primary.add(type);
            }
        }
        return new NodeTypeIteratorAdapter(primary);
    }

    @Override
    public NodeTypeIterator getMixinNodeTypes() throws RepositoryException {
        Collection<NodeType> mixin = new ArrayList<NodeType>();
        for (NodeType type : types.values()) {
            if (type.isMixin()) {
                mixin.add(type);
            }
        }
        return new NodeTypeIteratorAdapter(mixin);
    }

    @Override
    public NodeTypeTemplate createNodeTypeTemplate()
            throws UnsupportedRepositoryOperationException, RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public NodeTypeTemplate createNodeTypeTemplate(NodeTypeDefinition ntd)
            throws UnsupportedRepositoryOperationException, RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public NodeDefinitionTemplate createNodeDefinitionTemplate()
            throws UnsupportedRepositoryOperationException, RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public PropertyDefinitionTemplate createPropertyDefinitionTemplate()
            throws UnsupportedRepositoryOperationException, RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public NodeType registerNodeType(NodeTypeDefinition ntd, boolean allowUpdate)
            throws InvalidNodeTypeDefinitionException, NodeTypeExistsException,
            UnsupportedRepositoryOperationException, RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public NodeTypeIterator registerNodeTypes(NodeTypeDefinition[] ntds,
            boolean allowUpdate) throws InvalidNodeTypeDefinitionException,
            NodeTypeExistsException, UnsupportedRepositoryOperationException,
            RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public void unregisterNodeType(String name)
            throws UnsupportedRepositoryOperationException,
            NoSuchNodeTypeException, RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public void unregisterNodeTypes(String[] names)
            throws UnsupportedRepositoryOperationException,
            NoSuchNodeTypeException, RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

}
