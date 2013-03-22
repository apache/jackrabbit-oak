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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.ItemDefinition;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * EffectiveNodeType... TODO
 */
public class EffectiveNodeType {

    private static final Logger log = LoggerFactory.getLogger(EffectiveNodeType.class);

    private final Collection<NodeType> nodeTypes;
    private final ReadOnlyNodeTypeManager ntMgr;

    private EffectiveNodeType(Collection<NodeType> nodeTypes, ReadOnlyNodeTypeManager ntMgr) {
        this.nodeTypes = nodeTypes;
        this.ntMgr = ntMgr;
    }

    static EffectiveNodeType create(Collection<NodeType> nodeTypes, ReadOnlyNodeTypeManager ntMgr) throws ConstraintViolationException {
        if (!isValid(nodeTypes)) {
            throw new ConstraintViolationException("Invalid effective node type");
        }
        return new EffectiveNodeType(nodeTypes, ntMgr);
    }

    private static boolean isValid(Collection<NodeType> nodeTypes) {
        // FIXME: add validation
        return true;
    }

    public Iterable<NodeType> getAllNodeTypes() {
        return nodeTypes;
    }

    /**
     * Determines whether this effective node type representation includes
     * (either through inheritance or aggregation) the given node type.
     *
     * @param nodeTypeName name of node type
     * @return {@code true} if the given node type is included, otherwise {@code false}.
     */
    public boolean includesNodeType(String nodeTypeName) {
        for (NodeType type : nodeTypes) {
            if (type.isNodeType(nodeTypeName)) {
                return true;
            }
        }
        return false;
    }


    /**
     * Determines whether this effective node type representation includes
     * (either through inheritance or aggregation) all of the given node types.
     *
     * @param nodeTypeNames array of node type names
     * @return {@code true} if all of the given node types are included,
     *         otherwise {@code false}
     */
    public boolean includesNodeTypes(String[] nodeTypeNames) {
        for (String ntName : nodeTypeNames) {
            if (!includesNodeType(ntName)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Determines whether this effective node type supports adding
     * the specified mixin.
     * @param mixin name of mixin type
     * @return {@code true} if the mixin type is supported, otherwise {@code false}
     */
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

    public Iterable<NodeDefinition> getNodeDefinitions() {
        List<NodeDefinition> definitions = new ArrayList<NodeDefinition>();
        for (NodeType nt : nodeTypes) {
            definitions.addAll(((NodeTypeImpl) nt).internalGetChildDefinitions());
        }
        return definitions;
    }

    public Iterable<PropertyDefinition> getPropertyDefinitions() {
        List<PropertyDefinition> definitions = new ArrayList<PropertyDefinition>();
        for (NodeType nt : nodeTypes) {
            definitions.addAll(((NodeTypeImpl) nt).internalGetPropertyDefinitions());
        }
        return definitions;
    }

    public Iterable<NodeDefinition> getAutoCreateNodeDefinitions() {
        return Iterables.filter(getNodeDefinitions(), new Predicate<NodeDefinition>() {
            @Override
            public boolean apply(NodeDefinition nodeDefinition) {
                return nodeDefinition.isAutoCreated();
            }
        });
    }

    public Iterable<PropertyDefinition> getAutoCreatePropertyDefinitions() {
        return Iterables.filter(getPropertyDefinitions(), new Predicate<PropertyDefinition>() {
            @Override
            public boolean apply(PropertyDefinition propertyDefinition) {
                return propertyDefinition.isAutoCreated();
            }
        });
    }

    public Iterable<NodeDefinition> getMandatoryNodeDefinitions() {
        return Iterables.filter(getNodeDefinitions(), new Predicate<NodeDefinition>() {
            @Override
            public boolean apply(NodeDefinition nodeDefinition) {
                return nodeDefinition.isMandatory();
            }
        });
    }

    public Iterable<PropertyDefinition> getMandatoryPropertyDefinitions() {
        return Iterables.filter(getPropertyDefinitions(), new Predicate<PropertyDefinition>() {
            @Override
            public boolean apply(PropertyDefinition propertyDefinition) {
                return propertyDefinition.isMandatory();
            }
        });
    }

    /**
     * Return all node definitions that match the specified oak name.
     *
     * @param oakName An internal oak name.
     * @return All node definitions that match the given internal oak name.
     */
    @Nonnull
    public Iterable<NodeDefinition> getNamedNodeDefinitions(String oakName) {
        return Iterables.filter(getNodeDefinitions(), new DefinitionNamePredicate(oakName));
    }

    /**
     * Return all property definitions that match the specified oak name.
     *
     * @param oakName An internal oak name.
     * @return All property definitions that match the given internal oak name.
     */
    @Nonnull
    public Iterable<PropertyDefinition> getNamedPropertyDefinitions(String oakName) {
        return Iterables.filter(getPropertyDefinitions(), new DefinitionNamePredicate(oakName));
    }

    /**
     * Return all residual node definitions.
     *
     * @return All residual node definitions.
     */
    @Nonnull
    public Iterable<NodeDefinition> getResidualNodeDefinitions() {
        return Iterables.filter(getNodeDefinitions(), new Predicate<NodeDefinition>() {
            @Override
            public boolean apply(NodeDefinition nodeDefinition) {
                return NodeTypeConstants.RESIDUAL_NAME.equals(nodeDefinition.getName());
            }
        });
    }

    /**
     * Return all residual property definitions.
     *
     * @return All residual property definitions.
     */
    @Nonnull
    public Iterable<PropertyDefinition> getResidualPropertyDefinitions() {
        return Iterables.filter(getPropertyDefinitions(), new Predicate<PropertyDefinition>() {
            @Override
            public boolean apply(PropertyDefinition propertyDefinition) {
                return NodeTypeConstants.RESIDUAL_NAME.equals(propertyDefinition.getName());
            }
        });
    }

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

    public void checkRemoveProperty(PropertyState property) throws RepositoryException {
        PropertyDefinition definition = getDefinition(property);
        if (definition.isProtected()) {
            return;
        }

        if (!definition.getDeclaringNodeType().canRemoveProperty(property.getName())) {
            throw new ConstraintViolationException("Cannot remove property '" + property.getName() + '\'');
        }
    }

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

    public void checkRemoveNode(String name, NodeType nodeType) throws RepositoryException {
        NodeDefinition definition = getDefinition(name, nodeType);

        if (definition.isProtected()) {
            return;
        }

        if (!definition.getDeclaringNodeType().canRemoveNode(name)) {
            throw new ConstraintViolationException("Cannot remove node '" + name + '\'');
        }
    }

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

    public void checkOrderableChildNodes() throws UnsupportedRepositoryOperationException {
        Iterable<NodeType> nts = getAllNodeTypes();
        for (NodeType nt : nts) {
            if (nt.hasOrderableChildNodes()) {
                return;
            }
        }

        throw new UnsupportedRepositoryOperationException("Child node ordering is not supported on this node");
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

    private static class DefinitionNamePredicate implements Predicate<ItemDefinition> {

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
