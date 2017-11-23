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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.any;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_REQUIREDTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VALUECONSTRAINTS;
import static org.apache.jackrabbit.JcrConstants.MIX_REFERENCEABLE;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.commons.UUIDUtils.isValidUUID;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.plugins.nodetype.constraint.Constraints.valueConstraint;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.PropertyType;
import javax.jcr.Value;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.osgi.annotation.versioning.ConsumerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validator implementation that check JCR node type constraints.
 *
 * TODO: check protected properties and the structure they enforce. some of
 *       those checks may have to go into separate validator classes. This class
 *       should only perform checks based on node type information. E.g. it
 *       cannot and should not check whether the value of the protected jcr:uuid
 *       is unique.
 */
public class TypeEditor extends DefaultEditor {
    
    /**
     * Extension point that allows pluggable handling of constraint violations
     */
    @ConsumerType
    public static interface ConstraintViolationCallback {
        /**
         * Invoked whenever a constraint violation is detected.
         * 
         * <p>Implementors may choose to throw a {@link org.apache.jackrabbit.oak.api.CommitFailedException} or to handle the error
         * internally, for instance by logging.</p>
         * 
         * <p>Implementors may <b>not</b> throw other exception types from this method.</p>
         * 
         * @param path the path where the constraint violation was detected
         * @param nodeTypeNames the node type names of the node
         * @param code the error code
         * @param message the descriptive error message
         * @throws CommitFailedException thrown when the implementation decides to stop further processing
         * 
         * @see TypeEditor#THROW_ON_CONSTRAINT_VIOLATION
         * @see TypeEditor#WARN_ON_CONSTRAINT_VIOLATION
         */
        void onConstraintViolation(String path, List<String> nodeTypeNames, int code, String message) throws CommitFailedException;
    }

    public static final ConstraintViolationCallback THROW_ON_CONSTRAINT_VIOLATION = new ConstraintViolationCallback() {
        
        @Override
        public void onConstraintViolation(String path, List<String> nodeTypeNames, int code, String message) throws CommitFailedException {
            
            String fullPath =  path + '[' + nodeTypeNames.toString() + ']';

            throw new CommitFailedException(CONSTRAINT, code, fullPath + ": " + message);
        }
    };
    
    public static final ConstraintViolationCallback WARN_ON_CONSTRAINT_VIOLATION = new ConstraintViolationCallback() {
        
        @Override
        public void onConstraintViolation(String path, List<String> nodeTypeNames, int code, String message ) {
            
            String fullPath =  path + '[' + nodeTypeNames.toString() + ']';

            log.warn(new CommitFailedException(CONSTRAINT, code, fullPath + ": " + message).getMessage());

        }
    };
    
    /**
     * Creates a new <tt>TypeEditor</tt> instance
     * 
     * @param callback the callback to use when a constraint violation is found. The client must
     *   check the results of the callback invocations if the specified callback does not
     *   immediately propagate constraint violations as checked exceptions.
     * @param typesToCheck the types to check for. If <tt>null</tt>, this node is checked. Otherwise
     *  it is checked if its primary type or one of it's mixin types is contained in this parameters
     * @param types the <tt>/jcr:system/jcr:nodeTypes</tt> node
     * @param primary the node's primary type
     * @param mixins the node's mixins
     * @param builder a builder containing the current state of the node to check. May be used to set
     *  a default primary type if none is set
     * @return a new TypeEditor instance
     * @throws CommitFailedException when the primary type of mixin definition is incorrect
     */
    public static TypeEditor create(@Nonnull ConstraintViolationCallback callback, Set<String> typesToCheck,
            @Nonnull NodeState types, String primary, Iterable<String> mixins, 
            @Nonnull NodeBuilder builder) throws CommitFailedException {
        return new TypeEditor(callback, typesToCheck, types, primary, mixins, builder);
    }

    private static final Logger log = LoggerFactory.getLogger(TypeEditor.class);

    private final Set<String> typesToCheck;

    private boolean checkThisNode;

    private final ConstraintViolationCallback callback;
    
    private final TypeEditor parent;

    private final String nodeName;

    private final NodeState types;

    private final EffectiveType effective;

    private final NodeBuilder builder;

    private final boolean validate;
    
    TypeEditor(
            ConstraintViolationCallback callback, Set<String> typesToCheck, NodeState types,
            String primary, Iterable<String> mixins, NodeBuilder builder)
            throws CommitFailedException {
        this.callback = checkNotNull(callback);
        this.typesToCheck = typesToCheck;
        this.checkThisNode =
                typesToCheck == null
                || typesToCheck.contains(primary)
                || any(mixins, in(typesToCheck));
        this.parent = null;
        this.nodeName = null;
        this.types = checkNotNull(types);
        this.effective = createEffectiveType(null, null, primary, mixins);
        this.builder = checkNotNull(builder);
        this.validate = false;
    }

    private TypeEditor(
            @Nonnull TypeEditor parent, @Nonnull String name,
            @CheckForNull String primary, @Nonnull Iterable<String> mixins, @Nonnull NodeBuilder builder,
            boolean validate)
            throws CommitFailedException {
        this.callback= parent.callback;
        this.typesToCheck = parent.typesToCheck;
        this.checkThisNode =
                typesToCheck == null
                || typesToCheck.contains(primary)
                || any(mixins, in(typesToCheck));
        this.parent = checkNotNull(parent);
        this.nodeName = checkNotNull(name);
        this.types = parent.types;
        this.effective = createEffectiveType(parent.effective, name, primary, mixins);
        this.builder = checkNotNull(builder);
        this.validate = validate;
    }

    /**
     * Test constructor.
     */
    TypeEditor(EffectiveType effective) {
        this.callback = TypeEditor.THROW_ON_CONSTRAINT_VIOLATION;
        this.typesToCheck = null;
        this.checkThisNode = true;
        this.parent = null;
        this.nodeName = null;
        this.types = EMPTY_NODE;
        this.effective = checkNotNull(effective);
        this.builder = EMPTY_NODE.builder();
        this.validate = false;
    }

    /**
     * Throws or logs the specified constraint violation.
     *
     * @param code code of this violation
     * @param message description of the violation
     * @throws CommitFailedException the constraint violation
     */
    private void constraintViolation(int code, String message) throws CommitFailedException {
        List<String> nodeTypeNames = effective != null ? effective.getDirectTypeNames() : Collections.emptyList();
        
        callback.onConstraintViolation(getPath(), nodeTypeNames, code, message);
    }

    private String getPath() {
        if (parent == null) {
            return "/";
        } else if (parent.parent == null) {
            return '/' + nodeName;
        } else {
            return parent.getPath() + '/' + nodeName;
        }
    }

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        propertyChanged(null, after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        if (checkThisNode) {
            checkPropertyTypeConstraints(after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        String name = before.getName();
        if (checkThisNode && effective.isMandatoryProperty(name)) {
            constraintViolation(
                    22, "Mandatory property " + name + " can not be removed");
        }
    }

    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
        if (checkThisNode && validate) {
            // when adding a new node, or changing node type on an existing
            // node, we need to reverify type constraints
            checkNodeTypeConstraints(after);
            checkThisNode = false;
        }
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        // TODO: add any auto-created items that are still missing
        return childNodeChanged(name, MISSING_NODE, after);
    }

    @Override
    public TypeEditor childNodeChanged(
            String name, NodeState before, NodeState after)
            throws CommitFailedException {
        String primary = after.getName(JCR_PRIMARYTYPE);
        Iterable<String> mixins = after.getNames(JCR_MIXINTYPES);

        if (primary == null && effective != null) {
            // no primary type defined, find and apply a default type
            primary = effective.getDefaultType(name);
            if (primary != null) {
                builder.setProperty(JCR_PRIMARYTYPE, primary, NAME);
            } else {
                constraintViolation(
                        4, "No default primary type available "
                        + " for child node " + name);
            }
        }

        // if node type didn't change no need to validate child node
        boolean validate = primaryChanged(before, primary) || mixinsChanged(before, mixins);
        NodeBuilder childBuilder = builder.getChildNode(name);
        TypeEditor editor = new TypeEditor(this, name, primary, mixins, childBuilder, validate);
        if (checkThisNode && validate && !effective.isValidChildNode(name, editor.getEffective())) {
            constraintViolation(
                    1, "No matching definition found for child node " + name
                    + " with effective type " + editor.getEffective());
        }
        return editor;
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        if (checkThisNode && effective.isMandatoryChildNode(name)) {
            constraintViolation(26, "Mandatory child node " + name + " can not be removed");
        }
        return null; // no further checking needed for the removed subtree
    }

    //-----------------------------------------------------------< private >--
    @Nonnull
    private EffectiveType createEffectiveType(
            @CheckForNull EffectiveType parent, @CheckForNull String name,
            @CheckForNull String primary, @Nonnull Iterable<String> mixins)
            throws CommitFailedException {
        List<NodeState> list = Lists.newArrayList();

        NodeState type = (primary == null) ? null : types.getChildNode(primary);
        if (type == null || !type.exists()) {
            constraintViolation(1, "The primary type " + primary + " does not exist");
        } else if (type.getBoolean(JCR_ISMIXIN)) {
            constraintViolation(2, "Mixin type " + primary + " used as the primary type");
        } else {
            if (type.getBoolean(JCR_IS_ABSTRACT)) {
                if (parent != null && name != null && primary.equals(parent.getDefaultType(name))) {
                    // OAK-1013: Allow (with a warning) an abstract primary
                    // type if it's the default type implied by the parent node
                    log.warn("Abstract type " + primary
                            + " used as the default primary type of node "
                            + getPath());
                } else {
                    constraintViolation(2, "Abstract type " + primary + " used as the primary type");
                }
            }
            list.add(type);
        }

        // mixin types
        for (String mixin : mixins) {
            type = types.getChildNode(mixin);
            if (!type.exists()) {
                constraintViolation(5, "The mixin type " + mixin + " does not exist");
            } else if (!type.getBoolean(JCR_ISMIXIN)) {
                constraintViolation(6, "Primary type " + mixin + " used as a mixin type");
            } else if (type.getBoolean(JCR_IS_ABSTRACT)) {
                constraintViolation(7, "Abstract type " + mixin + " used as a mixin type");
            } else {
                list.add(type);
            }
        }

        return new EffectiveType(list);
    }

    @Nonnull
    private EffectiveType getEffective() {
        return effective;
    }

    private static int getRequiredType(NodeState definition) {
        int type = PropertyType.UNDEFINED;
        PropertyState required = definition.getProperty(JCR_REQUIREDTYPE);
        if (required != null) {
            String value = required.getValue(STRING);
            if ("BINARY".equals(value)) {
                type = PropertyType.BINARY;
            } else if ("BOOLEAN".equals(value)) {
                type = PropertyType.BOOLEAN;
            } else if ("DATE".equals(value)) {
                type = PropertyType.DATE;
            } else if ("DECIMAL".equals(value)) {
                type = PropertyType.DECIMAL;
            } else if ("DOUBLE".equals(value)) {
                type = PropertyType.DOUBLE;
            } else if ("LONG".equals(value)) {
                type = PropertyType.LONG;
            } else if ("NAME".equals(value)) {
                type = PropertyType.NAME;
            } else if ("PATH".equals(value)) {
                type = PropertyType.PATH;
            } else if ("REFERENCE".equals(value)) {
                type = PropertyType.REFERENCE;
            } else if ("STRING".equals(value)) {
                type = PropertyType.STRING;
            } else if ("URI".equals(value)) {
                type = PropertyType.URI;
            } else if ("WEAKREFERENCE".equals(value)) {
                type = PropertyType.WEAKREFERENCE;
            }
        }
        return type;
    }

    private void checkRequiredType(PropertyState property, int requiredType) throws CommitFailedException {
        if (requiredType != property.getType().tag()) {
            constraintViolation(55, "Required property type violation in " + property);
        }
    }

    private void checkValueConstraints(NodeState definition, PropertyState property, int requiredType) throws CommitFailedException {
        if (property.count() == 0) {
            return;
        }

        PropertyState constraints = definition.getProperty(JCR_VALUECONSTRAINTS);
        if (constraints == null || constraints.count() == 0) {
            return;
        }

        for (String constraint : constraints.getValue(STRINGS)) {
            Predicate<Value> predicate = valueConstraint(requiredType, constraint);
            for (Value v : ValueFactoryImpl.createValues(property, NamePathMapper.DEFAULT)) {
                if (predicate.apply(v)) {
                    return;
                }
            }
        }
        constraintViolation(5, "Value constraint violation in " + property);
    }

    private static boolean primaryChanged(NodeState before, String after) {
        String pre = before.getName(JCR_PRIMARYTYPE);
        return !Objects.equal(pre, after);
    }

    private static boolean mixinsChanged(NodeState before, Iterable<String> after) {
        List<String> pre = Lists.newArrayList(before.getNames(JCR_MIXINTYPES));
        Collections.sort(pre);
        List<String> post = Lists.newArrayList(after);
        Collections.sort(post);
        if (pre.isEmpty() && post.isEmpty()) {
            return false;
        } else if (pre.isEmpty() || post.isEmpty()) {
            return true;
        } else {
            return !Iterables.elementsEqual(pre, post);
        }
    }

    private void checkNodeTypeConstraints(NodeState after) throws CommitFailedException {
        EffectiveType effective = getEffective();

        Set<String> properties = effective.getMandatoryProperties();
        for (PropertyState ps : after.getProperties()) {
            properties.remove(ps.getName());
            checkPropertyTypeConstraints(ps);
        }
        // verify the presence of all mandatory items
        if (!properties.isEmpty()) {
            constraintViolation(21, "Mandatory property " + properties.iterator().next() + " not found in a new node");
        }

        List<String> names = Lists.newArrayList(after.getChildNodeNames());
        for (String child : effective.getMandatoryChildNodes()) {
            if (!names.remove(child)) {
                constraintViolation(25, "Mandatory child node " + child + " not found in a new node");
            }
        }
        if (!names.isEmpty()) {
            for (String name : names) {
                NodeState child = after.getChildNode(name);
                String primary = child.getName(JCR_PRIMARYTYPE);
                Iterable<String> mixins = child.getNames(JCR_MIXINTYPES);
                NodeBuilder childBuilder = builder.getChildNode(name);
                TypeEditor editor = new TypeEditor(this, name, primary, mixins, childBuilder, false);
                if (!effective.isValidChildNode(name, editor.getEffective())) {
                    constraintViolation(25, "Unexpected child node " + names + " found in a new node");
                }
            }
        }
    }

    private void checkPropertyTypeConstraints(PropertyState after)
            throws CommitFailedException {
        if (NodeStateUtils.isHidden(after.getName())) {
            return;
        }
        NodeState definition = effective.getDefinition(after);
        if (definition == null) {
            constraintViolation(4, "No matching property definition found for " + after);
        } else if (JCR_UUID.equals(after.getName()) && effective.isNodeType(MIX_REFERENCEABLE)) {
            // special handling for the jcr:uuid property of mix:referenceable
            // TODO: this should be done in a pluggable extension
            if (!isValidUUID(after.getValue(Type.STRING))) {
                constraintViolation(12, "Invalid UUID value in the jcr:uuid property");
            }
        } else {
            int requiredType = getRequiredType(definition);
            if (requiredType != PropertyType.UNDEFINED) {
                checkRequiredType(after, requiredType);
                checkValueConstraints(definition, after, requiredType);
            }
        }
    }

}
