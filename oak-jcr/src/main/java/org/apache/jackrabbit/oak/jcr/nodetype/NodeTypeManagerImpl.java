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
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
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

import org.apache.jackrabbit.commons.cnd.CompactNodeTypeDefReader;
import org.apache.jackrabbit.commons.cnd.DefinitionBuilderFactory;
import org.apache.jackrabbit.commons.cnd.DefinitionBuilderFactory.AbstractNodeDefinitionBuilder;
import org.apache.jackrabbit.commons.cnd.DefinitionBuilderFactory.AbstractNodeTypeDefinitionBuilder;
import org.apache.jackrabbit.commons.cnd.DefinitionBuilderFactory.AbstractPropertyDefinitionBuilder;
import org.apache.jackrabbit.commons.cnd.ParseException;
import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.jcr.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.namepath.NameMapper;

public class NodeTypeManagerImpl implements NodeTypeManager {

    private final ValueFactoryImpl vf;
    private final NameMapper mapper;
    private final List<NodeTypeDelegate> typeDelegates;

    private final Map<String, NodeType> typemap = new HashMap<String, NodeType>();

    public NodeTypeManagerImpl(ValueFactoryImpl vf, NameMapper mapper) throws RepositoryException {
        this.vf = vf;
        this.mapper = mapper;

        try {
            InputStream stream = NodeTypeManagerImpl.class.getResourceAsStream("builtin_nodetypes.cnd");
            Reader reader = new InputStreamReader(stream, "UTF-8");
            try {
                DefinitionBuilderFactory<NodeTypeDelegate, Map<String, String>> dbf = new DefinitionDelegateBuilderFactory(vf);
                CompactNodeTypeDefReader<NodeTypeDelegate, Map<String, String>> cndr = new CompactNodeTypeDefReader<NodeTypeDelegate, Map<String, String>>(
                        reader, null, dbf);

                typeDelegates = cndr.getNodeTypeDefinitions();
            } catch (ParseException ex) {
                throw new RepositoryException("Failed to load built-in node types", ex);
            } finally {
                stream.close();
            }
        } catch (IOException ex) {
            throw new RepositoryException("Failed to load built-in node types", ex);
        }
    }

    private void init() {
        if (typemap.isEmpty()) {
            for (NodeTypeDelegate t : typeDelegates) {
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
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public NodeTypeTemplate createNodeTypeTemplate(NodeTypeDefinition ntd) throws RepositoryException {

        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public NodeDefinitionTemplate createNodeDefinitionTemplate() throws RepositoryException {

        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public PropertyDefinitionTemplate createPropertyDefinitionTemplate() throws RepositoryException {

        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public NodeType registerNodeType(NodeTypeDefinition ntd, boolean allowUpdate) throws RepositoryException {

        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public NodeTypeIterator registerNodeTypes(NodeTypeDefinition[] ntds, boolean allowUpdate) throws RepositoryException {

        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public void unregisterNodeType(String name) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public void unregisterNodeTypes(String[] names) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    private class DefinitionDelegateBuilderFactory extends DefinitionBuilderFactory<NodeTypeDelegate, Map<String, String>> {

        private Map<String, String> nsmap = new HashMap<String, String>();

        private final ValueFactoryImpl vf;

        public DefinitionDelegateBuilderFactory(ValueFactoryImpl vf) {
            this.vf = vf;
        }

        @Override
        public Map<String, String> getNamespaceMapping() {
            return nsmap;
        }

        @Override
        public AbstractNodeTypeDefinitionBuilder<NodeTypeDelegate> newNodeTypeDefinitionBuilder() throws RepositoryException {
            return new NodeTypeDefinitionDelegateBuilder(vf);
        }

        @Override
        public void setNamespace(String prefix, String uri) throws RepositoryException {
            nsmap.put(prefix, uri);
        }

        @Override
        public void setNamespaceMapping(Map<String, String> nsmap) {
            this.nsmap = nsmap;
        }
    }

    private class NodeTypeDefinitionDelegateBuilder extends AbstractNodeTypeDefinitionBuilder<NodeTypeDelegate> {

        private List<PropertyDefinitionDelegateBuilder> propertyDefinitions = new ArrayList<PropertyDefinitionDelegateBuilder>();
        private List<NodeDefinitionDelegateBuilder> childNodeDefinitions = new ArrayList<NodeDefinitionDelegateBuilder>();

        private final ValueFactoryImpl vf;

        private String primaryItemName;
        private List<String> declaredSuperTypes = new ArrayList<String>();

        public NodeTypeDefinitionDelegateBuilder(ValueFactoryImpl vf) {
            this.vf = vf;
        }

        @Override
        public void addSupertype(String superType) throws RepositoryException {
            this.declaredSuperTypes.add(superType);
        }

        @Override
        public void setPrimaryItemName(String primaryItemName) throws RepositoryException {
            this.primaryItemName = primaryItemName;
        }

        @Override
        public AbstractPropertyDefinitionBuilder<NodeTypeDelegate> newPropertyDefinitionBuilder() throws RepositoryException {
            return new PropertyDefinitionDelegateBuilder(this);
        }

        @Override
        public AbstractNodeDefinitionBuilder<NodeTypeDelegate> newNodeDefinitionBuilder() throws RepositoryException {
            return new NodeDefinitionDelegateBuilder(this);
        }

        @Override
        public NodeTypeDelegate build() throws RepositoryException {

            NodeTypeDelegate result = new NodeTypeDelegate(name, declaredSuperTypes.toArray(new String[declaredSuperTypes.size()]),
                    primaryItemName, isMixin, isAbstract, isOrderable);

            for (PropertyDefinitionDelegateBuilder pdb : propertyDefinitions) {
                result.addPropertyDefinitionDelegate(pdb.getPropertyDefinitionDelegate(vf.getCoreValueFactory()));
            }

            for (NodeDefinitionDelegateBuilder ndb : childNodeDefinitions) {
                result.addChildNodeDefinitionDelegate(ndb.getNodeDefinitionDelegate());
            }

            return result;
        }

        public void addPropertyDefinition(PropertyDefinitionDelegateBuilder pd) {
            this.propertyDefinitions.add(pd);
        }

        public void addNodeDefinition(NodeDefinitionDelegateBuilder nd) {
            this.childNodeDefinitions.add(nd);
        }
    }

    private class NodeDefinitionDelegateBuilder extends AbstractNodeDefinitionBuilder<NodeTypeDelegate> {

        private String declaringNodeType;
        private String defaultPrimaryType;
        private List<String> requiredPrimaryTypes = new ArrayList<String>();

        private final NodeTypeDefinitionDelegateBuilder ndtb;

        public NodeDefinitionDelegateBuilder(NodeTypeDefinitionDelegateBuilder ntdb) {
            this.ndtb = ntdb;
        }

        public NodeDefinitionDelegate getNodeDefinitionDelegate() {
            return new NodeDefinitionDelegate(name, autocreate, isMandatory, onParent, isProtected,
                    requiredPrimaryTypes.toArray(new String[requiredPrimaryTypes.size()]), defaultPrimaryType, allowSns);
        };

        @Override
        public void setDefaultPrimaryType(String defaultPrimaryType) throws RepositoryException {
            this.defaultPrimaryType = defaultPrimaryType;
        }

        @Override
        public void addRequiredPrimaryType(String name) throws RepositoryException {
            this.requiredPrimaryTypes.add(name);
        }

        @Override
        public void setDeclaringNodeType(String declaringNodeType) throws RepositoryException {
            this.declaringNodeType = declaringNodeType;
        }

        @Override
        public void build() throws RepositoryException {
            this.ndtb.addNodeDefinition(this);
        }
    }

    private class PropertyDefinitionDelegateBuilder extends AbstractPropertyDefinitionBuilder<NodeTypeDelegate> {

        private String declaringNodeType;
        private List<String> defaultValues = new ArrayList<String>();
        private List<String> valueConstraints = new ArrayList<String>();

        private final NodeTypeDefinitionDelegateBuilder ndtb;

        public PropertyDefinitionDelegateBuilder(NodeTypeDefinitionDelegateBuilder ntdb) {
            this.ndtb = ntdb;
        }

        public PropertyDefinitionDelegate getPropertyDefinitionDelegate(CoreValueFactory cvf) {

            CoreValue[] defaultCoreValues = new CoreValue[defaultValues.size()];

            for (int i = 0; i < defaultCoreValues.length; i++) {
                defaultCoreValues[i] = cvf.createValue(defaultValues.get(i), requiredType);
            }

            return new PropertyDefinitionDelegate(name, autocreate, isMandatory, onParent, isProtected, requiredType, isMultiple,
                    defaultCoreValues);
        }

        @Override
        public void addValueConstraint(String constraint) throws RepositoryException {
            this.valueConstraints.add(constraint);
        }

        @Override
        public void addDefaultValues(String value) throws RepositoryException {
            this.defaultValues.add(value);
        }

        @Override
        public void setDeclaringNodeType(String declaringNodeType) throws RepositoryException {
            this.declaringNodeType = declaringNodeType;
        }

        @Override
        public void build() throws RepositoryException {
            this.ndtb.addPropertyDefinition(this);
        }
    }
}
