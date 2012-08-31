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
package org.apache.jackrabbit.oak.plugins.type;

import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;

class TypeValidator implements Validator {
    private static final Logger log = LoggerFactory.getLogger(TypeValidator.class);

    private final NodeTypeManager ntm;
    private final Tree parent;

    private EffectiveNodeType parentType;

    @Nonnull
    private EffectiveNodeType getParentType() throws CommitFailedException {
        if (parentType == null) {
            parentType = getEffectiveNodeType(parent);
        }
        return parentType;
    }

    public TypeValidator(NodeTypeManager ntm, Tree parent) {
        this.ntm = ntm;
        this.parent = parent;
    }

    //-------------------------------------------------------< NodeValidator >

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        validateType(after);
        if (!getParentType().canAddProperty(after)) {
            throwConstraintViolationException(
                    "Can't add property " + after.getName() + " at " + parent.getPath());
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        validateType(after);
        if (!getParentType().canSetProperty(after)) {
            throwConstraintViolationException(
                    "Can't set property " + after.getName() + " at " + parent.getPath());
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        if (!getParentType().canRemoveProperty(before)) {
            throwConstraintViolationException(
                    "Can't delete property " + before.getName() + " at " + parent.getPath());
        }
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        if (!getParentType().canAddChildNode(name, after)) {
            throwConstraintViolationException(
                    "Can't add node " + name + " at " + parent.getPath());
        }
        return new TypeValidator(ntm, new ReadOnlyTree(after));
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        if (!getParentType().canChangeChildNode(name, after)) {
            throwConstraintViolationException(
                    "Can't modify node " + name + " at " + parent.getPath());
        }
        return new TypeValidator(ntm, new ReadOnlyTree(after));
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        if (!getParentType().canRemoveNode(name, before)) {
            throwConstraintViolationException(
                    "Can't delete node " + name + " at " + parent.getPath());
        }
        return new TypeValidator(ntm, new ReadOnlyTree(before));
    }

    //------------------------------------------------------------< private >---

    private void validateType(PropertyState after) throws CommitFailedException {
        boolean primaryType = JCR_PRIMARYTYPE.equals(after.getName());
        boolean mixinType = JCR_MIXINTYPES.equals(after.getName());
        if (primaryType || mixinType) {
            try {
                for (CoreValue cv : after.getValues()) {
                    String ntName = cv.getString();
                    NodeType nt = ntm.getNodeType(ntName);
                    if (nt.isAbstract()) {
                        throwConstraintViolationException("Can't create node with abstract type: " + ntName);
                    }
                    if (primaryType && nt.isMixin()) {
                        throwConstraintViolationException("Can't assign mixin for primary type: " + ntName);
                    }
                    if (mixinType && !nt.isMixin()) {
                        throwConstraintViolationException("Can't assign primary type for mixin: " + ntName);
                    }
                }
            }
            catch (RepositoryException e) {
                throwConstraintViolationException(e);
            }
        }
    }

    private NodeType getPrimaryType(Tree tree) throws CommitFailedException {
        try {
            PropertyState jcrPrimaryType = tree.getProperty(JCR_PRIMARYTYPE);
            if (jcrPrimaryType != null) {
                for (CoreValue typeName : jcrPrimaryType.getValues()) {
                    String ntName = typeName.getString();
                    NodeType type = ntm.getNodeType(ntName);
                    if (type == null) {
                        log.warn("Could not find node type {} for item at {}", ntName, tree.getPath());
                    }
                    return type;
                }
            }
            log.warn("Item at {} has no primary type", tree.getPath());
            return null;
        }
        catch (RepositoryException e) {
            return throwConstraintViolationException(e);
        }
    }

    private List<NodeType> getMixinTypes(Tree tree) throws CommitFailedException {
        try {
            List<NodeType> types = Lists.newArrayList();
            PropertyState jcrMixinType = tree.getProperty(JCR_MIXINTYPES);
            if (jcrMixinType != null) {
                for (CoreValue typeName : jcrMixinType.getValues()) {
                    String ntName = typeName.getString();
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
        catch (RepositoryException e) {
            return throwConstraintViolationException(e);
        }
    }

    private EffectiveNodeType getEffectiveNodeType(Tree tree) throws CommitFailedException {
        return new EffectiveNodeType(getPrimaryType(tree), getMixinTypes(tree));
    }

    private static <T> T throwConstraintViolationException(String message) throws CommitFailedException {
        throw new CommitFailedException(new ConstraintViolationException(message));
    }

    private static <T> T throwConstraintViolationException(RepositoryException cause) throws CommitFailedException {
        throw new CommitFailedException(cause);
    }

    private class EffectiveNodeType {
        private final NodeType primaryType;
        private final List<NodeType> mixinTypes;

        public EffectiveNodeType(NodeType primaryType, List<NodeType> mixinTypes) {
            this.primaryType = primaryType;
            this.mixinTypes = mixinTypes;
        }

        public boolean canAddProperty(PropertyState property) {
            return true; // todo implement canAddProperty
        }

        public boolean canSetProperty(PropertyState property) {
            return true; // todo implement canSetProperty
        }

        public boolean canRemoveProperty(PropertyState property) {
            return true; // todo implement canRemoveProperty
        }

        public boolean canAddChildNode(String name, NodeState node) {
            return true; // todo implement canAddChildNode
        }

        public boolean canChangeChildNode(String name, NodeState node) {
            return true; // todo implement canChangeChildNode
        }

        public boolean canRemoveNode(String name, NodeState node) {
            return true; // todo implement canRemoveNode
        }
    }

}
