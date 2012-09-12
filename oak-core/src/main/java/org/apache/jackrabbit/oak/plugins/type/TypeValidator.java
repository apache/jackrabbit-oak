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

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.Binary;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;
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
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.ISO8601;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;

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

        ReadOnlyTree addedTree = new ReadOnlyTree(parent, name, after);
        EffectiveNodeType addedType = getEffectiveNodeType(addedTree);
        addedType.checkMandatoryItems(addedTree);
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
            log.warn("Item at {} has no primary type. Assuming nt:unstructured", tree.getPath());
            return ntm.getNodeType(NT_UNSTRUCTURED);
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

    private static class EffectiveNodeType {
        private final Iterable<NodeType> allTypes;

        public EffectiveNodeType(NodeType primaryType, List<NodeType> mixinTypes) {
            this.allTypes = Iterables.concat(mixinTypes, Collections.singleton(primaryType));
        }

        public boolean canSetProperty(PropertyState property) {
            return property.isArray()
                ? canSetProperty(property.getName(), property.getValues())
                : canSetProperty(property.getName(), property.getValue());
        }

        private boolean canSetProperty(final String propertyName, final List<CoreValue> values) {
            Value[] jcrValues = jcrValues(values);
            for (NodeType nodeType : allTypes) {
                if (nodeType.canSetProperty(propertyName, jcrValues)) {
                    return true;
                }
            }
            return false;
        }

        private boolean canSetProperty(final String propertyName, final CoreValue value) {
            Value jcrValue = jcrValue(value);
            for (NodeType nodeType : allTypes) {
                if (nodeType.canSetProperty(propertyName, jcrValue)) {
                    return true;
                }
            }
            return false;
        }

        public boolean canRemoveProperty(PropertyState property) {
            final String name = property.getName();
            for (NodeType nodeType : allTypes) {
                if (nodeType.canRemoveProperty(name)) {
                    return true;
                }
            }
            return false;
        }

        public boolean canRemoveNode(final String name) {
            for (NodeType nodeType : allTypes) {
                if (nodeType.canRemoveProperty(name)) {
                    return true;
                }
            }
            return false;
        }

        public boolean canAddChildNode(final String name) {
            for (NodeType nodeType : allTypes) {
                if (nodeType.canAddChildNode(name)) {
                    return true;
                }
            }
            return false;
        }

        public boolean canAddChildNode(final String name, final String ntName) {
            for (NodeType nodeType : allTypes) {
                if (nodeType.canAddChildNode(name, ntName)) {
                    return true;
                }
            }
            return false;
        }

        public void checkMandatoryItems(final ReadOnlyTree tree) throws CommitFailedException {
            for (NodeType nodeType : allTypes) {
                for (PropertyDefinition pd : nodeType.getPropertyDefinitions()) {
                    String name = pd.getName();
                    if (pd.isMandatory() && !pd.isProtected() && tree.getProperty(name) == null) {
                        throwConstraintViolationException(name + " in " + nodeType.getName() + " is mandatory");
                    }
                }
                for (NodeDefinition nd : nodeType.getChildNodeDefinitions()) {
                    String name = nd.getName();
                    if (nd.isMandatory() && !nd.isProtected() && tree.getChild(name) == null) {
                        throwConstraintViolationException(name + " in " + nodeType.getName() + " is mandatory");
                    }
                }
            }
        }
    }

    private static Value[] jcrValues(List<CoreValue> values) {
            Value[] jcrValues = new  Value[values.size()];

            int k = 0;
            for (CoreValue value : values) {
                jcrValues[k++] = jcrValue(value);
            }

            return jcrValues;
        }

        private static Value jcrValue(final CoreValue value) {
            return new JcrValue(value);
        }

    private static class JcrValue implements Value {
        private final CoreValue value;

        public JcrValue(CoreValue value) {
            this.value = value;
        }

        @Override
        public String getString() {
            return value.getString();
        }

        @Override
        public InputStream getStream() {
            return value.getNewStream();
        }

        @Override
        public Binary getBinary() {
            return new JcrBinary(value);
        }

        @Override
        public long getLong() {
            return value.getLong();
        }

        @Override
        public double getDouble() {
            return value.getDouble();
        }

        @Override
        public BigDecimal getDecimal() {
            return value.getDecimal();
        }

        @Override
        public Calendar getDate() throws ValueFormatException {
            Calendar cal = ISO8601.parse(getString());
            if (cal == null) {
                throw new ValueFormatException("Not a date string: " + getString());
            }
            return cal;
        }

        @Override
        public boolean getBoolean() {
            return value.getBoolean();
        }

        @Override
        public int getType() {
            return value.getType();
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof JcrValue && value.equals(((JcrValue) other).value);
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }

    private static class JcrBinary implements Binary {
        private final CoreValue value;

        public JcrBinary(CoreValue value) {
            this.value = value;
        }

        @Override
        public InputStream getStream() {
            return value.getNewStream();
        }

        @Override
        public int read(byte[] b, long position) throws IOException {
            InputStream stream = value.getNewStream();
            try {
                if (position != stream.skip(position)) {
                    throw new IOException("Can't skip to position " + position);
                }
                return stream.read(b);
            }
            finally {
                stream.close();
            }
        }

        @Override
        public long getSize() {
            return value.length();
        }

        @Override
        public void dispose() {
        }
    }
}
