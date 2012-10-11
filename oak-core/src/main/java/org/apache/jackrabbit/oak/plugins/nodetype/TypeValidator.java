/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.nodetype;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.PropertyDefinition;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.value.ValueImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

/**
 * Validator implementation that check JCR node type constraints.
 *
 * TODO: check protected properties and the structure they enforce. some of
 *       those checks may have to go into separate validator classes. This class
 *       should only perform checks based on node type information. E.g. it
 *       cannot and should not check whether the value of the protected jcr:uuid
 *       is unique.
 */
class TypeValidator implements Validator {
    private static final Logger log = LoggerFactory.getLogger(TypeValidator.class);

    private final NodeTypeManager ntm;
    private final ReadOnlyTree parent;
    private final NamePathMapper mapper;

    private EffectiveNodeType parentType;

    @Nonnull
    private EffectiveNodeType getParentType() throws RepositoryException {
        if (parentType == null) {
            parentType = getEffectiveNodeType(parent);
        }
        return parentType;
    }

    public TypeValidator(NodeTypeManager ntm, ReadOnlyTree parent, NamePathMapper mapper) {
        this.ntm = ntm;
        this.parent = parent;
        this.mapper = mapper;
    }

    //-------------------------------------------------------< NodeValidator >

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        try {
            checkType(after);
            getParentType().checkSetProperty(after);
        }
        catch (RepositoryException e) {
            throw new CommitFailedException(
                    "Cannot add property '" + after.getName() + "' at " + parent.getPath(), e);
        }
        catch (IllegalStateException e) {
            throw new CommitFailedException(
                    "Cannot add property '" + after.getName() + "' at " + parent.getPath(), e);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        try {
            checkType(after);
            getParentType().checkSetProperty(after);
        }
        catch (RepositoryException e) {
            throw new CommitFailedException(
                    "Cannot set property '" + after.getName() + "' at " + parent.getPath(), e);
        }
        catch (IllegalStateException e) {
            throw new CommitFailedException(
                    "Cannot set property '" + after.getName() + "' at " + parent.getPath(), e);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        try {
            getParentType().checkRemoveProperty(before);
        }
        catch (RepositoryException e) {
            throw new CommitFailedException(
                    "Cannot remove property '" + before.getName() + "' at " + parent.getPath(), e);
        }
        catch (IllegalStateException e) {
            throw new CommitFailedException(
                    "Cannot remove property '" + before.getName() + "' at " + parent.getPath(), e);
        }
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        try {
            PropertyState type = after.getProperty(JCR_PRIMARYTYPE);
            if (type == null || type.count() == 0) {
                getParentType().canAddChildNode(name);
            }
            else {
                String ntName = type.getValue(STRING, 0);
                getParentType().checkAddChildNode(name, ntName);
            }

            ReadOnlyTree addedTree = new ReadOnlyTree(parent, name, after);
            EffectiveNodeType addedType = getEffectiveNodeType(addedTree);
            addedType.checkMandatoryItems(addedTree);
            return new TypeValidator(ntm, new ReadOnlyTree(parent, name, after), mapper);
        }
        catch (RepositoryException e) {
            throw new CommitFailedException(
                    "Cannot add node '" + name + "' at " + parent.getPath(), e);
        }
        catch (IllegalStateException e) {
            throw new CommitFailedException(
                    "Cannot add node '" + name + "' at " + parent.getPath(), e);
        }
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        return new TypeValidator(ntm, new ReadOnlyTree(parent, name, after), mapper);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        try {
            getParentType().checkRemoveNode(name);
            return null;
        }
        catch (RepositoryException e) {
            throw new CommitFailedException(
                    "Cannot remove node '" + name + "' at " + parent.getPath(), e);
        }
        catch (IllegalStateException e) {
            throw new CommitFailedException(
                    "Cannot add node '" + name + "' at " + parent.getPath(), e);
        }
    }

    //------------------------------------------------------------< private >---

    private void checkType(PropertyState after) throws RepositoryException {
        boolean primaryType = JCR_PRIMARYTYPE.equals(after.getName());
        boolean mixinType = JCR_MIXINTYPES.equals(after.getName());
        if (primaryType || mixinType) {
            for (String ntName : after.getValue(STRINGS)) {
                NodeType nt = ntm.getNodeType(ntName);
                if (nt.isAbstract()) {
                    throw new ConstraintViolationException("Can't create node with abstract type: " + ntName);
                }
                if (primaryType && nt.isMixin()) {
                    throw new ConstraintViolationException("Can't assign mixin for primary type: " + ntName);
                }
                if (mixinType && !nt.isMixin()) {
                    throw new ConstraintViolationException("Can't assign primary type for mixin: " + ntName);
                }
            }
        }
    }

    private NodeType getPrimaryType(Tree tree) throws RepositoryException {
        PropertyState jcrPrimaryType = tree.getProperty(JCR_PRIMARYTYPE);
        if (jcrPrimaryType != null) {
            for (String ntName : jcrPrimaryType.getValue(STRINGS)) {
                NodeType type = ntm.getNodeType(ntName);
                if (type == null) {
                    log.warn("Could not find node type {} for item at {}", ntName, tree.getPath());
                }
                return type;
            }
        }
        log.warn("Item at {} has no primary type. Assuming nt:unstructured", tree.getPath());
        return ntm.getNodeType(NT_UNSTRUCTURED);
    }

    private List<NodeType> getMixinTypes(Tree tree) throws RepositoryException {
        List<NodeType> types = Lists.newArrayList();
        PropertyState jcrMixinType = tree.getProperty(JCR_MIXINTYPES);
        if (jcrMixinType != null) {
            for (String ntName : jcrMixinType.getValue(STRINGS)) {
                NodeType type = ntm.getNodeType(ntName);
                if (type == null) {
                    log.warn("Could not find mixin type {} for item at {}", ntName, tree.getPath());
                }
                else {
                    types.add(type);
                }
            }
        }
        return types;
    }

    private EffectiveNodeType getEffectiveNodeType(Tree tree) throws RepositoryException {
        return new EffectiveNodeType(getPrimaryType(tree), getMixinTypes(tree));
    }

    private class EffectiveNodeType {
        private final Iterable<NodeType> allTypes;

        public EffectiveNodeType(NodeType primaryType, List<NodeType> mixinTypes) {
            this.allTypes = Iterables.concat(mixinTypes, Collections.singleton(primaryType));
        }

        public void checkSetProperty(PropertyState property) throws ConstraintViolationException {
            if (isProtected(property.getName())) {
                return;
            }
            if (property.isArray()) {
                List<Value> values = ValueFactoryImpl.createValues(property, mapper);
                checkSetProperty(property.getName(), values);
            }
            else {
                Value value = ValueFactoryImpl.createValue(property, mapper);
                checkSetProperty(property.getName(), value);
            }
        }

        private void checkSetProperty(final String propertyName, List<Value> values)
                throws ConstraintViolationException {
            Value[] valueArray = values.toArray(new Value[values.size()]);
            for (NodeType nodeType : allTypes) {
                if (nodeType.canSetProperty(propertyName, valueArray)) {
                    return;
                }
            }
            throw new ConstraintViolationException("Cannot set property '" + propertyName + "' to '" + values + '\'');
        }

        private void checkSetProperty(final String propertyName, Value value) throws ConstraintViolationException {
            for (NodeType nodeType : allTypes) {
                if (nodeType.canSetProperty(propertyName, value)) {
                    return;
                }
            }
            throw new ConstraintViolationException("Cannot set property '" + propertyName + "' to '" + value + '\'');
        }

        public void checkRemoveProperty(PropertyState property) throws ConstraintViolationException {
            if (isProtected(property.getName())) {
                return;
            }
            final String name = property.getName();
            for (NodeType nodeType : allTypes) {
                if (nodeType.canRemoveProperty(name)) {
                    return;
                }
            }
            throw new ConstraintViolationException("Cannot remove property '" + property.getName() + '\'');
        }

        public void checkRemoveNode(final String name) throws ConstraintViolationException {
            for (NodeType nodeType : allTypes) {
                if (nodeType.canRemoveNode(name)) {
                    return;
                }
            }
            throw new ConstraintViolationException("Cannot remove node '" + name + '\'');
        }

        public void canAddChildNode(final String name) throws ConstraintViolationException {
            for (NodeType nodeType : allTypes) {
                if (nodeType.canAddChildNode(name)) {
                    return;
                }
            }
            throw new ConstraintViolationException("Cannot add node '" + name + '\'');
        }

        public void checkAddChildNode(final String name, final String ntName) throws ConstraintViolationException {
            for (NodeType nodeType : allTypes) {
                if (nodeType.canAddChildNode(name, ntName)) {
                    return;
                }
            }
            throw new ConstraintViolationException("Cannot add node '" + name + "' of type '" + ntName + '\'');
        }

        public void checkMandatoryItems(final ReadOnlyTree tree) throws ConstraintViolationException {
            for (NodeType nodeType : allTypes) {
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

        private boolean isProtected(String propertyName) {
            for (NodeType nodeType : allTypes) {
                for (PropertyDefinition pd : nodeType.getPropertyDefinitions()) {
                    if (propertyName.equals(pd.getName()) && pd.isProtected()) {
                        return true;
                    }
                }
            }
            return false;
        }

        private Value[] jcrValues(List<CoreValue> values) {
            Value[] jcrValues = new  Value[values.size()];

            int k = 0;
            for (CoreValue value : values) {
                jcrValues[k++] = jcrValue(value);
            }

            return jcrValues;
        }

        private Value jcrValue(CoreValue value) {
            return new ValueImpl(value, mapper);
        }

    }

}
