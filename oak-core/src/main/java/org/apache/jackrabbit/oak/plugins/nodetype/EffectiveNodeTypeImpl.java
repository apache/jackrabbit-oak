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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.ItemDefinition;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EffectiveNodeTypeImpl...
 *
 * TODO implementation needs optimization
 */
class EffectiveNodeTypeImpl implements EffectiveNodeType {

    private static final Logger log = LoggerFactory.getLogger(EffectiveNodeTypeImpl.class);

    private final Collection<NodeType> nodeTypes;
    private final ReadOnlyNodeTypeManager ntMgr;

    private EffectiveNodeTypeImpl(Collection<NodeType> nodeTypes, ReadOnlyNodeTypeManager ntMgr) {
        this.nodeTypes = nodeTypes;
        this.ntMgr = ntMgr;
    }

    static EffectiveNodeType create(Collection<NodeType> nodeTypes, ReadOnlyNodeTypeManager ntMgr) throws ConstraintViolationException {
        if (!isValid(nodeTypes)) {
            throw new ConstraintViolationException("Invalid effective node type");
        }
        return new EffectiveNodeTypeImpl(nodeTypes, ntMgr);
    }

    private static boolean isValid(Collection<NodeType> nodeTypes) {
        // FIXME: add validation
        return true;
    }

    //---------------------------------------------=----< EffectiveNodeType >---
    @Override
    public Iterable<NodeType> getAllNodeTypes() {
        return nodeTypes;
    }

    @Override
    public boolean includesNodeType(String nodeTypeName) {
        for (NodeType type : nodeTypes) {
            if (type.isNodeType(nodeTypeName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean includesNodeTypes(String[] nodeTypeNames) {
        for (String ntName : nodeTypeNames) {
            if (!includesNodeType(ntName)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean supportsMixin(String mixin) {
        if (includesNodeType(mixin)) {
            return true;
        }

        NodeType mixinType = null;
        try {
            mixinType = ntMgr.internalGetNodeType(mixin);
            if (!mixinType.isMixin() || mixinType.isAbstract()) {
                return false;
            }
        } catch (NoSuchNodeTypeException e) {
            log.debug("Unknown mixin type " + mixin);
        }

        if (mixinType != null) {
            Set<NodeType> newTypes = new HashSet<NodeType>(nodeTypes);
            newTypes.add(mixinType);
            return isValid(newTypes);
        }
        return false;
    }

    @Override
    public Iterable<NodeDefinition> getNodeDefinitions() {
        List<NodeDefinition> definitions = new ArrayList<NodeDefinition>();
        for (NodeType nt : nodeTypes) {
            definitions.addAll(((NodeTypeImpl) nt).internalGetChildDefinitions());
        }
        return definitions;
    }

    @Override
    public Iterable<PropertyDefinition> getPropertyDefinitions() {
        List<PropertyDefinition> definitions = new ArrayList<PropertyDefinition>();
        for (NodeType nt : nodeTypes) {
            definitions.addAll(((NodeTypeImpl) nt).internalGetPropertyDefinitions());
        }
        return definitions;
    }

    @Override
    public Iterable<NodeDefinition> getAutoCreateNodeDefinitions() {
        return Iterables.filter(getNodeDefinitions(), new Predicate<NodeDefinition>() {
            @Override
            public boolean apply(NodeDefinition nodeDefinition) {
                return nodeDefinition.isAutoCreated();
            }
        });
    }

    @Override
    public Iterable<PropertyDefinition> getAutoCreatePropertyDefinitions() {
        return Iterables.filter(getPropertyDefinitions(), new Predicate<PropertyDefinition>() {
            @Override
            public boolean apply(PropertyDefinition propertyDefinition) {
                return propertyDefinition.isAutoCreated();
            }
        });
    }

    @Override
    public Iterable<NodeDefinition> getMandatoryNodeDefinitions() {
        return Iterables.filter(getNodeDefinitions(), new Predicate<NodeDefinition>() {
            @Override
            public boolean apply(NodeDefinition nodeDefinition) {
                return nodeDefinition.isMandatory();
            }
        });
    }

    @Override
    public Iterable<PropertyDefinition> getMandatoryPropertyDefinitions() {
        return Iterables.filter(getPropertyDefinitions(), new Predicate<PropertyDefinition>() {
            @Override
            public boolean apply(PropertyDefinition propertyDefinition) {
                return propertyDefinition.isMandatory();
            }
        });
    }

    @Override
    public Iterable<NodeDefinition> getNamedNodeDefinitions(String oakName) {
        return Iterables.filter(getNodeDefinitions(), new DefinitionNamePredicate(oakName));
    }

    @Override
    public Iterable<PropertyDefinition> getNamedPropertyDefinitions(String oakName) {
        return Iterables.filter(getPropertyDefinitions(), new DefinitionNamePredicate(oakName));
    }

    @Override
    public Iterable<NodeDefinition> getResidualNodeDefinitions() {
        return Iterables.filter(getNodeDefinitions(), new Predicate<NodeDefinition>() {
            @Override
            public boolean apply(NodeDefinition nodeDefinition) {
                return NodeTypeConstants.RESIDUAL_NAME.equals(nodeDefinition.getName());
            }
        });
    }

    @Override
    public Iterable<PropertyDefinition> getResidualPropertyDefinitions() {
        return Iterables.filter(getPropertyDefinitions(), new Predicate<PropertyDefinition>() {
            @Override
            public boolean apply(PropertyDefinition propertyDefinition) {
                return NodeTypeConstants.RESIDUAL_NAME.equals(propertyDefinition.getName());
            }
        });
    }

    @Override
    public void checkSetProperty(PropertyState property) throws RepositoryException {
        PropertyDefinition definition = getDefinition(property);
        if (definition.isProtected()) {
            return;
        }

        NodeType nt = definition.getDeclaringNodeType();
        if (definition.isMultiple()) {
            List<Value> values = ValueFactoryImpl.createValues(property, ntMgr.getNamePathMapper());
            if (!nt.canSetProperty(property.getName(), values.toArray(new Value[values.size()]))) {
                throw new ConstraintViolationException("Cannot set property '" + property.getName() + "' to '" + values + '\'');
            }
        } else {
            Value v = ValueFactoryImpl.createValue(property, ntMgr.getNamePathMapper());
            if (!nt.canSetProperty(property.getName(), v)) {
                throw new ConstraintViolationException("Cannot set property '" + property.getName() + "' to '" + v + '\'');
            }
        }
    }

    @Override
    public void checkRemoveProperty(PropertyState property) throws RepositoryException {
        PropertyDefinition definition = getDefinition(property);
        if (definition.isProtected()) {
            return;
        }

        if (!definition.getDeclaringNodeType().canRemoveProperty(property.getName())) {
            throw new ConstraintViolationException("Cannot remove property '" + property.getName() + '\'');
        }
    }

    @Override
    public void checkAddChildNode(String name, NodeType nodeType) throws RepositoryException {
        NodeDefinition definition = getDefinition(name, nodeType);

        if (definition.isProtected()) {
            return;
        }

        if (nodeType == null) {
            if (!definition.getDeclaringNodeType().canAddChildNode(name)) {
                throw new ConstraintViolationException("Cannot add node '" + name + '\'');
            }
        } else {
            if (!definition.getDeclaringNodeType().canAddChildNode(name, nodeType.getName())) {
                throw new ConstraintViolationException("Cannot add node '" + name + "' of type '" + nodeType.getName() + '\'');
            }
        }
    }

    @Override
    public void checkRemoveNode(String name, NodeType nodeType) throws RepositoryException {
        NodeDefinition definition = getDefinition(name, nodeType);

        if (definition.isProtected()) {
            return;
        }

        if (!definition.getDeclaringNodeType().canRemoveNode(name)) {
            throw new ConstraintViolationException("Cannot remove node '" + name + '\'');
        }
    }

    @Override
    public void checkMandatoryItems(Tree tree) throws ConstraintViolationException {
        for (NodeType nodeType : nodeTypes) {
            for (PropertyDefinition pd : nodeType.getPropertyDefinitions()) {
                String name = pd.getName();
                if (pd.isMandatory() && !pd.isProtected() && tree.getProperty(name) == null) {
                    throw new ConstraintViolationException(
                            "Property '" + name + "' in '" + nodeType.getName() + "' is mandatory");
                }
            }
            for (NodeDefinition nd : nodeType.getChildNodeDefinitions()) {
                String name = nd.getName();
                if (nd.isMandatory() && !nd.isProtected() && tree.getChild(name) == null) {
                    throw new ConstraintViolationException(
                            "Node '" + name + "' in '" + nodeType.getName() + "' is mandatory");
                }
            }
        }
    }

    //------------------------------------------------------------< private >---

    private PropertyDefinition getDefinition(PropertyState property) throws RepositoryException {
        String propertyName = property.getName();
        int propertyType = property.getType().tag();
        boolean isMultiple = property.isArray();

        return ntMgr.getDefinition(nodeTypes, propertyName, isMultiple, propertyType, true);
    }

    private NodeDefinition getDefinition(String nodeName, NodeType nodeType) throws ConstraintViolationException {
        // FIXME: ugly hack to workaround sns-hack that was used to map sns-item definitions with node types.
        String nameToCheck = nodeName;
        if (nodeName.startsWith("jcr:childNodeDefinition") && !nodeName.equals("jcr:childNodeDefinition")) {
            nameToCheck = nodeName.substring(0, "jcr:childNodeDefinition".length());
        }
        if (nodeName.startsWith("jcr:propertyDefinition") && !nodeName.equals("jcr:propertyDefinition")) {
            nameToCheck = nodeName.substring(0, "jcr:propertyDefinition".length());
        }
        return ntMgr.getDefinition(nodeTypes, nameToCheck, nodeType);
    }

    private class DefinitionNamePredicate implements Predicate<ItemDefinition> {

        private final String oakName;

        DefinitionNamePredicate(String oakName) {
            this.oakName = oakName;
        }
        @Override
        public boolean apply(@Nullable ItemDefinition definition) {
            return definition instanceof ItemDefinitionImpl && ((ItemDefinitionImpl) definition).getOakName().equals(oakName);
        }
    }
}
