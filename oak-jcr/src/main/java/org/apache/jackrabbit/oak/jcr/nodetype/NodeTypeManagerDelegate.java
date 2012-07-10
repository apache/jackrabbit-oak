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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.commons.cnd.CompactNodeTypeDefReader;
import org.apache.jackrabbit.commons.cnd.DefinitionBuilderFactory;
import org.apache.jackrabbit.commons.cnd.DefinitionBuilderFactory.AbstractNodeDefinitionBuilder;
import org.apache.jackrabbit.commons.cnd.DefinitionBuilderFactory.AbstractNodeTypeDefinitionBuilder;
import org.apache.jackrabbit.commons.cnd.DefinitionBuilderFactory.AbstractPropertyDefinitionBuilder;
import org.apache.jackrabbit.commons.cnd.ParseException;

public class NodeTypeManagerDelegate {

    private final List<NodeTypeDelegate> typeDelegates;

    private static final Map<String, String> nsdefaults;
    static {
        Map<String, String> tmp = new HashMap<String,String>();
        tmp.put("rep", "internal"); // TODO: https://issues.apache.org/jira/browse/OAK-74
        tmp.put("jcr", "http://www.jcp.org/jcr/1.0");
        tmp.put("nt",  "http://www.jcp.org/jcr/nt/1.0");
        tmp.put("mix", "http://www.jcp.org/jcr/mix/1.0");
        tmp.put("xml", "http://www.w3.org/XML/1998/namespace");
        nsdefaults = Collections.unmodifiableMap(tmp);
    }

    public NodeTypeManagerDelegate() throws RepositoryException {
        try {
            InputStream stream = NodeTypeManagerImpl.class.getResourceAsStream("builtin_nodetypes.cnd");
            Reader reader = new InputStreamReader(stream, "UTF-8");
            try {
                DefinitionBuilderFactory<NodeTypeDelegate, Map<String, String>> dbf = new DefinitionDelegateBuilderFactory();
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

    public List<NodeTypeDelegate> getAllNodeTypeDelegates() {
        return typeDelegates;
    }

    private class DefinitionDelegateBuilderFactory extends DefinitionBuilderFactory<NodeTypeDelegate, Map<String, String>> {

        private Map<String, String> nsmap = new HashMap<String, String>();

        @Override
        public Map<String, String> getNamespaceMapping() {
            return nsmap;
        }

        @Override
        public AbstractNodeTypeDefinitionBuilder<NodeTypeDelegate> newNodeTypeDefinitionBuilder() throws RepositoryException {
            return new NodeTypeDefinitionDelegateBuilder(this);
        }

        @Override
        public void setNamespace(String prefix, String uri) throws RepositoryException {

            String t = nsdefaults.get(prefix);
            if (t == null) {
                throw new RepositoryException("Unsupported namespace prefix for initial CND load: " + prefix);
            }
            else if (!t.equals(uri)) {
                throw new RepositoryException("Can't remap namespace prefix for initial CND laod: " + prefix);
            }

            nsmap.put(prefix, uri);
        }

        @Override
        public void setNamespaceMapping(Map<String, String> nsmap) {
            this.nsmap = nsmap;
        }

        public String convertNameToOak(String cndName) throws RepositoryException {
            return cndName; // is assumed to be the Oak name for now
        }

        public List<String> convertNamesToOak(List<String> cndNames) throws RepositoryException {
            List<String> result = new ArrayList<String>();
            for (String cndName : cndNames) {
                result.add(convertNameToOak(cndName));
            }
            return result;
        }
    }

    private class NodeTypeDefinitionDelegateBuilder extends AbstractNodeTypeDefinitionBuilder<NodeTypeDelegate> {

        private final List<PropertyDefinitionDelegateBuilder> propertyDefinitions = new ArrayList<PropertyDefinitionDelegateBuilder>();
        private final List<NodeDefinitionDelegateBuilder> childNodeDefinitions = new ArrayList<NodeDefinitionDelegateBuilder>();

        private final DefinitionDelegateBuilderFactory ddbf;

        private String primaryItemName;
        private List<String> declaredSuperTypes = new ArrayList<String>();

        public NodeTypeDefinitionDelegateBuilder(DefinitionDelegateBuilderFactory ddbf) {
            this.ddbf = ddbf;
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

            name = ddbf.convertNameToOak(name);
            declaredSuperTypes = ddbf.convertNamesToOak(declaredSuperTypes);
            primaryItemName = ddbf.convertNameToOak(primaryItemName);

            NodeTypeDelegate result = new NodeTypeDelegate(name, declaredSuperTypes.toArray(new String[declaredSuperTypes.size()]),
                    primaryItemName, isMixin, isAbstract, isOrderable);

            for (PropertyDefinitionDelegateBuilder pdb : propertyDefinitions) {
                result.addPropertyDefinitionDelegate(pdb.getPropertyDefinitionDelegate());
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

        public String convertNameToOak(String name) throws RepositoryException {
            return ddbf.convertNameToOak(name);
        }
    }

    private class NodeDefinitionDelegateBuilder extends AbstractNodeDefinitionBuilder<NodeTypeDelegate> {

        private String declaringNodeType;
        private String defaultPrimaryType;
        private final List<String> requiredPrimaryTypes = new ArrayList<String>();

        private final NodeTypeDefinitionDelegateBuilder ndtb;

        public NodeDefinitionDelegateBuilder(NodeTypeDefinitionDelegateBuilder ntdb) {
            this.ndtb = ntdb;
        }

        public NodeDefinitionDelegate getNodeDefinitionDelegate() {
            return new NodeDefinitionDelegate(name, autocreate, isMandatory, onParent, isProtected,
                    requiredPrimaryTypes.toArray(new String[requiredPrimaryTypes.size()]), defaultPrimaryType, allowSns);
        }

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
        private final List<String> defaultValues = new ArrayList<String>();
        private final List<String> valueConstraints = new ArrayList<String>();

        private final NodeTypeDefinitionDelegateBuilder ndtb;

        public PropertyDefinitionDelegateBuilder(NodeTypeDefinitionDelegateBuilder ntdb) {
            this.ndtb = ntdb;
        }

        public PropertyDefinitionDelegate getPropertyDefinitionDelegate() throws RepositoryException {

            name = ndtb.convertNameToOak(name);

            return new PropertyDefinitionDelegate(name, autocreate, isMandatory, onParent, isProtected, requiredType, isMultiple,
                    defaultValues);
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
