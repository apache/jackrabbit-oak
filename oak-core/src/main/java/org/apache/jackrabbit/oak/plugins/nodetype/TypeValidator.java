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

import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
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

    private final ReadOnlyNodeTypeManager ntm;
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

    public TypeValidator(ReadOnlyNodeTypeManager ntm, ReadOnlyTree parent, NamePathMapper mapper) {
        this.ntm = ntm;
        this.parent = parent;
        this.mapper = mapper;
    }

    //----------------------------------------------------------< Validator >---

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        if (isHidden(after)) {
            return;
        }
        try {
            checkPrimaryAndMixinTypes(after);
            getParentType().checkSetProperty(after);
        } catch (RepositoryException e) {
            throw new CommitFailedException("Cannot add property '" + after.getName() + "' at " + parent.getPath(), e);
        } catch (IllegalStateException e) {
            throw new CommitFailedException("Cannot add property '" + after.getName() + "' at " + parent.getPath(), e);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        if (isHidden(after)) {
            return;
        }
        try {
            checkPrimaryAndMixinTypes(after);
            getParentType().checkSetProperty(after);
        } catch (RepositoryException e) {
            throw new CommitFailedException("Cannot set property '" + after.getName() + "' at " + parent.getPath(), e);
        } catch (IllegalStateException e) {
            throw new CommitFailedException("Cannot set property '" + after.getName() + "' at " + parent.getPath(), e);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        if (isHidden(before)) {
            return;
        }
        try {
            getParentType().checkRemoveProperty(before);
        } catch (RepositoryException e) {
            throw new CommitFailedException("Cannot remove property '" + before.getName() + "' at " + parent.getPath(), e);
        } catch (IllegalStateException e) {
            throw new CommitFailedException("Cannot remove property '" + before.getName() + "' at " + parent.getPath(), e);
        }
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        try {
            getParentType().checkAddChildNode(name, getNodeType(after));

            ReadOnlyTree addedTree = new ReadOnlyTree(parent, name, after);
            EffectiveNodeType addedType = getEffectiveNodeType(addedTree);
            addedType.checkMandatoryItems(addedTree);
            return new TypeValidator(ntm, new ReadOnlyTree(parent, name, after), mapper);
        } catch (RepositoryException e) {
            throw new CommitFailedException("Cannot add node '" + name + "' at " + parent.getPath(), e);
        } catch (IllegalStateException e) {
            throw new CommitFailedException("Cannot add node '" + name + "' at " + parent.getPath(), e);
        }
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        return new TypeValidator(ntm, new ReadOnlyTree(parent, name, after), mapper);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        try {
            getParentType().checkRemoveNode(name, getNodeType(before));
            return null;
        } catch (RepositoryException e) {
            throw new CommitFailedException("Cannot remove node '" + name + "' at " + parent.getPath(), e);
        } catch (IllegalStateException e) {
            throw new CommitFailedException("Cannot add node '" + name + "' at " + parent.getPath(), e);
        }
    }

    //------------------------------------------------------------< private >---

    private void checkPrimaryAndMixinTypes(PropertyState after) throws RepositoryException {
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

    @CheckForNull
    private NodeType getNodeType(NodeState state) throws RepositoryException {
        PropertyState type = state.getProperty(JCR_PRIMARYTYPE);
        if (type == null || type.count() == 0) {
            // TODO: review again
            return null;
        } else {
            String ntName = type.getValue(STRING, 0);
            return ntm.getNodeType(ntName);
        }
    }

    private static boolean isHidden(PropertyState state) {
        return NodeStateUtils.isHidden(state.getName());
    }

    // FIXME: the same is also required on JCR level. probably keeping that in 1 single location would be preferable.
    private EffectiveNodeType getEffectiveNodeType(Tree tree) throws RepositoryException {
        return new EffectiveNodeType(ntm.getEffectiveNodeTypes(tree));
    }

    private class EffectiveNodeType {
        private final Iterable<NodeType> allTypes;

        public EffectiveNodeType(Iterable<NodeType> allTypes) {
            this.allTypes = allTypes;
        }

        public void checkSetProperty(PropertyState property) throws RepositoryException {
            PropertyDefinition definition = getDefinition(property);
            if (definition.isProtected()) {
                return;
            }

            NodeType nt = definition.getDeclaringNodeType();
            if (definition.isMultiple()) {
                List<Value> values = ValueFactoryImpl.createValues(property, mapper);
                if (!nt.canSetProperty(property.getName(), values.toArray(new Value[values.size()]))) {
                    throw new ConstraintViolationException("Cannot set property '" + property.getName() + "' to '" + values + '\'');
                }
            } else {
                Value v = ValueFactoryImpl.createValue(property, mapper);
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

        public void checkRemoveNode(String name, NodeType nodeType) throws RepositoryException {
            NodeDefinition definition = getDefinition(name, nodeType);
            if (definition.isProtected()) {
                return;
            }

            if (!definition.getDeclaringNodeType().canRemoveNode(name)) {
                throw new ConstraintViolationException("Cannot remove node '" + name + '\'');
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

        public void checkMandatoryItems(ReadOnlyTree tree) throws ConstraintViolationException {
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

        private PropertyDefinition getDefinition(PropertyState property) throws RepositoryException {
            String propertyName = property.getName();
            int propertyType = property.getType().tag();
            boolean isMultiple = property.isArray();

            return ntm.getDefinition(allTypes, propertyName, isMultiple, propertyType, true);
        }

        private NodeDefinition getDefinition(String nodeName, NodeType nodeType) throws RepositoryException {
            // FIXME: ugly hack to workaround sns-hack that was used to map sns-item definitions with node types.
            String nameToCheck = nodeName;
            if (nodeName.startsWith("jcr:childNodeDefinition") && !nodeName.equals("jcr:childNodeDefinition")) {
                nameToCheck = nodeName.substring(0, "jcr:childNodeDefinition".length());
            }
            if (nodeName.startsWith("jcr:propertyDefinition") && !nodeName.equals("jcr:propertyDefinition")) {
                nameToCheck = nodeName.substring(0, "jcr:propertyDefinition".length());
            }
            return ntm.getDefinition(allTypes, nameToCheck, nodeType);
        }


    }
}
