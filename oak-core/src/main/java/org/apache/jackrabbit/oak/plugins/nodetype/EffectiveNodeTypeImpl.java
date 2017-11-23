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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static javax.jcr.PropertyType.UNDEFINED;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.nodetype.EffectiveNodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * EffectiveNodeTypeImpl... TODO
 */
class EffectiveNodeTypeImpl implements EffectiveNodeType {

    private static final Logger log = LoggerFactory.getLogger(EffectiveNodeTypeImpl.class);

    private static final NodeTypeImpl[] NO_MIXINS = new NodeTypeImpl[0];

    private final Map<String, NodeTypeImpl> nodeTypes = Maps.newLinkedHashMap();

    private final ReadOnlyNodeTypeManager ntMgr;

    EffectiveNodeTypeImpl(
            NodeTypeImpl primary, NodeTypeImpl[] mixins,
            ReadOnlyNodeTypeManager ntMgr) {
        this.ntMgr = ntMgr;

        addNodeType(checkNotNull(primary));
        for (NodeTypeImpl mixin : checkNotNull(mixins)) {
            addNodeType(mixin);
        }
        if (!nodeTypes.containsKey(NT_BASE)) {
            try {
                nodeTypes.put(
                        NT_BASE,
                        (NodeTypeImpl) ntMgr.getNodeType(NT_BASE)); // FIXME
            } catch (RepositoryException e) {
                // TODO: ignore/warning/error?
            }
        }
    }

    EffectiveNodeTypeImpl(NodeTypeImpl primary, ReadOnlyNodeTypeManager ntMgr) {
        this(primary, NO_MIXINS, ntMgr);
    }

    private void addNodeType(NodeTypeImpl type) {
        String name = type.getName();
        if (!nodeTypes.containsKey(name)) {
            nodeTypes.put(name, type);
            NodeType[] supertypes = type.getDeclaredSupertypes();
            for (NodeType supertype : supertypes) {
                addNodeType((NodeTypeImpl) supertype); // FIXME
            }
        }
    }

    /**
     * Determines whether this effective node type representation includes
     * (either through inheritance or aggregation) the given node type.
     *
     * @param nodeTypeName name of node type
     * @return {@code true} if the given node type is included, otherwise {@code false}.
     */
    @Override
    public boolean includesNodeType(String nodeTypeName) {
        return nodeTypes.containsKey(nodeTypeName);
    }


    /**
     * Determines whether this effective node type representation includes
     * (either through inheritance or aggregation) all of the given node types.
     *
     * @param nodeTypeNames array of node type names
     * @return {@code true} if all of the given node types are included,
     *         otherwise {@code false}
     */
    @Override
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

        return true;
    }

    @Override
    public Iterable<NodeDefinition> getNodeDefinitions() {
        List<NodeDefinition> definitions = new ArrayList<NodeDefinition>();
        for (NodeType nt : nodeTypes.values()) {
            definitions.addAll(((NodeTypeImpl) nt).internalGetChildDefinitions());
        }
        return definitions;
    }

    @Override
    public Iterable<PropertyDefinition> getPropertyDefinitions() {
        List<PropertyDefinition> definitions = new ArrayList<PropertyDefinition>();
        for (NodeType nt : nodeTypes.values()) {
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

    /**
     * Return all node definitions that match the specified oak name.
     *
     * @param oakName An internal oak name.
     * @return All node definitions that match the given internal oak name.
     */
    @Override
    @Nonnull
    public Iterable<NodeDefinition> getNamedNodeDefinitions(
            final String oakName) {
        return Iterables.concat(Iterables.transform(
                nodeTypes.values(),
                new Function<NodeTypeImpl, Iterable<NodeDefinition>>() {
                    @Override
                    public Iterable<NodeDefinition> apply(NodeTypeImpl input) {
                        return input.getDeclaredNamedNodeDefinitions(oakName);
                    }
                }));
    }

    /**
     * Return all property definitions that match the specified oak name.
     *
     * @param oakName An internal oak name.
     * @return All property definitions that match the given internal oak name.
     */
    @Override
    @Nonnull
    public Iterable<PropertyDefinition> getNamedPropertyDefinitions(
            String oakName) {
        List<PropertyDefinition> definitions = newArrayList();
        for (NodeTypeImpl type : nodeTypes.values()) {
            definitions.addAll(type.getDeclaredNamedPropertyDefinitions(oakName));
        }
        return definitions;
    }

    /**
     * Return all residual node definitions.
     *
     * @return All residual node definitions.
     */
    @Override
    @Nonnull
    public Iterable<NodeDefinition> getResidualNodeDefinitions() {
        List<NodeDefinition> definitions = newArrayList();
        for (NodeTypeImpl type : nodeTypes.values()) {
            definitions.addAll(type.getDeclaredResidualNodeDefinitions());
        }
        return definitions;
    }

    /**
     * Return all residual property definitions.
     *
     * @return All residual property definitions.
     */
    @Override
    @Nonnull
    public Iterable<PropertyDefinition> getResidualPropertyDefinitions() {
        List<PropertyDefinition> definitions = newArrayList();
        for (NodeTypeImpl type : nodeTypes.values()) {
            definitions.addAll(type.getDeclaredResidualPropertyDefinitions());
        }
        return definitions;
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
    public void checkMandatoryItems(Tree tree) throws ConstraintViolationException {
        for (NodeType nodeType : nodeTypes.values()) {
            for (PropertyDefinition pd : nodeType.getPropertyDefinitions()) {
                String name = pd.getName();
                if (pd.isMandatory() && !pd.isProtected() && tree.getProperty(name) == null) {
                    throw new ConstraintViolationException(
                            "Property '" + name + "' in '" + nodeType.getName() + "' is mandatory");
                }
            }
            for (NodeDefinition nd : nodeType.getChildNodeDefinitions()) {
                String name = nd.getName();
                if (nd.isMandatory() && !nd.isProtected() && !tree.hasChild(name)) {
                    throw new ConstraintViolationException(
                            "Node '" + name + "' in '" + nodeType.getName() + "' is mandatory");
                }
            }
        }
    }

    @Override
    public void checkOrderableChildNodes() throws UnsupportedRepositoryOperationException {
        for (NodeType nt : nodeTypes.values()) {
            if (nt.hasOrderableChildNodes()) {
                return;
            }
        }

        throw new UnsupportedRepositoryOperationException("Child node ordering is not supported on this node");
    }

    /**
     * Calculates the applicable definition for the property with the specified
     * characteristics under a parent with this effective type.
     *
     * @param propertyName The internal oak name of the property for which the
     * definition should be retrieved.
     * @param isMultiple {@code true} if the target property is multi-valued.
     * @param type The target type of the property.
     * @param exactTypeMatch {@code true} if the required type of the definition
     * must exactly match the type of the target property.
     * @return the applicable definition for the target property.
     * @throws ConstraintViolationException If no matching definition can be found.
     */
    @Override
    public PropertyDefinition getPropertyDefinition(
            String propertyName, boolean isMultiple,
            int type, boolean exactTypeMatch)
            throws ConstraintViolationException {
       // TODO: This may need to be optimized
       for (PropertyDefinition def : getNamedPropertyDefinitions(propertyName)) {
           int defType = def.getRequiredType();
           if (isMultiple == def.isMultiple()
                   &&(!exactTypeMatch || (type == defType || UNDEFINED == type || UNDEFINED == defType))) {
               return def;
           }
       }

       // try if there is a residual definition
       for (PropertyDefinition def : getResidualPropertyDefinitions()) {
           int defType = def.getRequiredType();
           if (isMultiple == def.isMultiple()
                   && (!exactTypeMatch || (type == defType || UNDEFINED == type || UNDEFINED == defType))) {
               return def;
           }
       }

       throw new ConstraintViolationException(
               "No matching property definition found for " + propertyName);
   }

    /**
     * Calculates the applicable definition for the property with the specified
     * characteristics under a parent with this effective type.
     *
     * @param name The internal oak name of the property for which the definition should be retrieved.
     * @param type The target type of the property.
     * @param unknownMultiple {@code true} if the target property has an unknown type, {@code false} if it is known to be a multi-valued property.
     * @return the applicable definition for the target property or {@code null} if no matching definition can be found.
     */
    @Override
    public PropertyDefinition getPropertyDefinition(String name, int type, boolean unknownMultiple) {
        // TODO check multivalue handling
        Iterable<PropertyDefinition> definitions = getNamedPropertyDefinitions(name);

        for (PropertyDefinition def : definitions) {
            int requiredType = def.getRequiredType();
            if ((requiredType == PropertyType.UNDEFINED || type == PropertyType.UNDEFINED || requiredType == type)
                    && (def.isMultiple() || unknownMultiple)) {
                return def;
            }
        }
        definitions = getResidualPropertyDefinitions();
        for (PropertyDefinition def : definitions) {
            int requiredType = def.getRequiredType();
            if ((requiredType == PropertyType.UNDEFINED || type == PropertyType.UNDEFINED || requiredType == type)
                    && !def.isMultiple() && unknownMultiple) {
                return def;
            }
        }
        for (PropertyDefinition def : definitions) {
            int requiredType = def.getRequiredType();
            if ((requiredType == PropertyType.UNDEFINED || type == PropertyType.UNDEFINED || requiredType == type)
                    && def.isMultiple()) {
                return def;
            }
        }
        return null;
    }

    /**
     *
     * @param childName The internal oak name of the target node.
     * @param childEffective
     * @return the node definition
     * @throws ConstraintViolationException
     */
    @Override
    public NodeDefinition getNodeDefinition(
            String childName, EffectiveNodeType childEffective)
            throws ConstraintViolationException {
       for (NodeDefinition def : getNamedNodeDefinitions(childName)) {
           boolean match = true;
           if (childEffective != null && !childEffective.includesNodeTypes(def.getRequiredPrimaryTypeNames())) {
               match = false;
           }
           if (match) {
               return def;
           }
       }

       for (NodeDefinition def : getResidualNodeDefinitions()) {
           boolean match = true;
           if (childEffective != null && !childEffective.includesNodeTypes(def.getRequiredPrimaryTypeNames())) {
               match = false;
           }
           if (match) {
               return def;
           }
       }

       throw new ConstraintViolationException(
               "No matching node definition found for " + childName);
   }

    //------------------------------------------------------------< private >---

    private PropertyDefinition getDefinition(PropertyState property) throws RepositoryException {
        String propertyName = property.getName();
        int propertyType = property.getType().tag();
        boolean isMultiple = property.isArray();

        return getPropertyDefinition(propertyName, isMultiple, propertyType, true);
    }

}
