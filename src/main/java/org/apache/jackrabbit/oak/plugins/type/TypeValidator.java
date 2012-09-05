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

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
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
import static org.apache.jackrabbit.JcrConstants.NT_BASE;

class TypeValidator implements Validator {
    private static final Logger log = LoggerFactory.getLogger(TypeValidator.class);

    private final NodeTypeManager ntm;
    private final ReadOnlyTree parent;

    private EffectiveNodeType parentType;

    @Nonnull
    private EffectiveNodeType getParentType() throws CommitFailedException {
        if (parentType == null) {
            parentType = getEffectiveNodeType(parent);
        }
        return parentType;
    }

    public TypeValidator(NodeTypeManager ntm, ReadOnlyTree parent) {
        this.ntm = ntm;
        this.parent = parent;
    }

    //-------------------------------------------------------< NodeValidator >

    // TODO check presence of mandatory items

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        validateType(after);
        if (!getParentType().canSetProperty(after)) {
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
        PropertyState type = after.getProperty(JCR_PRIMARYTYPE);
        if (type == null || type.getValues().isEmpty()) {
            if (!getParentType().canAddChildNode(name)) {
                throwConstraintViolationException(
                        "Can't add node " + name + " at " + parent.getPath());
            }
        }
        else {
            String ntName = type.getValues().get(0).getString();
            if (!getParentType().canAddChildNode(name, ntName)) {
                throwConstraintViolationException(
                        "Can't add node " + name + " at " + parent.getPath());
            }
        }
        return new TypeValidator(ntm, new ReadOnlyTree(parent, name, after));
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        return new TypeValidator(ntm, new ReadOnlyTree(parent, name, after));
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        if (!getParentType().canRemoveNode(name)) {
            throwConstraintViolationException(
                    "Can't delete node " + name + " at " + parent.getPath());
        }
        return null;
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
            log.warn("Item at {} has no primary type. Assuming nt:base", tree.getPath());
            return ntm.getNodeType(NT_BASE);
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
        private final Iterable<NodeType> allTypes;

        public EffectiveNodeType(NodeType primaryType, List<NodeType> mixinTypes) {
            this.primaryType = primaryType;
            this.mixinTypes = mixinTypes;
            this.allTypes = Iterables.concat(mixinTypes, Collections.singleton(primaryType));
        }

        public boolean canSetProperty(PropertyState property) {
            return property.isArray()
                ? canSetProperty(property.getName(), property.getValues())
                : canSetProperty(property.getName(), property.getValue());
        }

        private boolean canSetProperty(final String propertyName, final List<CoreValue> values) {
            return Iterables.any(allTypes, new Predicate<NodeType>() {
                @Override
                public boolean apply(NodeType nt) {
                    return true;
                    // TODO return nt.canSetProperty(propertyName, values);
                }
            });
        }

        private boolean canSetProperty(final String propertyName, final CoreValue value) {
            return Iterables.any(allTypes, new Predicate<NodeType>() {
                @Override
                public boolean apply(NodeType nt) {
                    return true;
                    // TODO return nt.canSetProperty(propertyName, value);
                }
            });
        }

        public boolean canRemoveProperty(PropertyState property) {
            final String name = property.getName();
            return Iterables.any(allTypes, new Predicate<NodeType>() {
                @Override
                public boolean apply(NodeType nt) {
                    return nt.canRemoveProperty(name);
                }
            });
        }

        public boolean canRemoveNode(final String name) {
            return Iterables.any(allTypes, new Predicate<NodeType>() {
                @Override
                public boolean apply(NodeType nt) {
                    return nt.canRemoveProperty(name);
                }
            });
        }

        public boolean canAddChildNode(final String name) {
            return Iterables.any(allTypes, new Predicate<NodeType>() {
                @Override
                public boolean apply(NodeType nt) {
                    return nt.canAddChildNode(name);
                }
            });
        }

        public boolean canAddChildNode(final String name, final String ntName) {
            return Iterables.any(allTypes, new Predicate<NodeType>() {
                @Override
                public boolean apply(NodeType nt) {
                    return true;
                    // TODO return nt.canAddChildNode(name, ntName);
                }
            });
        }

    }

}
