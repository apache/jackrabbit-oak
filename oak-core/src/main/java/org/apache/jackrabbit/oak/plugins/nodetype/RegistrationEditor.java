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
import static org.apache.jackrabbit.JcrConstants.JCR_CHILDNODEDEFINITION;
import static org.apache.jackrabbit.JcrConstants.JCR_MULTIPLE;
import static org.apache.jackrabbit.JcrConstants.JCR_NAME;
import static org.apache.jackrabbit.JcrConstants.JCR_NODETYPENAME;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_PROPERTYDEFINITION;
import static org.apache.jackrabbit.JcrConstants.JCR_REQUIREDTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SUPERTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

import java.util.Queue;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

/**
 * Editor that validates the consistency of the in-content node type registry
 * under {@code /jcr:system/jcr:nodeTypes} and maintains the access-optimized
 * version uncer {@code /jcr:system/oak:nodeTypes}.
 *
 * <ul>
 *     <li>validate new definitions</li>
 *     <li>detect collisions,</li>
 *     <li>prevent circular inheritance,</li>
 *     <li>reject modifications to definitions that render existing content invalid,</li>
 *     <li>prevent un-registration of built-in node types.</li>
 * </ul>
 */
class RegistrationEditor extends DefaultEditor {

    private final NodeBuilder builder;

    private final Set<String> changedTypes = Sets.newHashSet();

    private final Set<String> removedTypes = Sets.newHashSet();

    RegistrationEditor(NodeBuilder builder) {
        this.builder = checkNotNull(builder);
    }

    private void validateAndCompile(String name, NodeState after)
            throws CommitFailedException {
        NodeBuilder types = builder.child(JCR_SYSTEM).child(JCR_NODE_TYPES);

        String path = NODE_TYPES_PATH + "/" + name;
        NodeBuilder type = types.child(name);

        // - jcr:nodeTypeName (NAME) protected mandatory
        PropertyState nodeTypeName = after.getProperty(JCR_NODETYPENAME);
        if (nodeTypeName == null
                || !name.equals(nodeTypeName.getValue(NAME))) {
            throw new CommitFailedException(
                    "Unexpected " + JCR_NODETYPENAME + " in " + path);
        }

        // - jcr:supertypes (NAME) protected multiple
        PropertyState supertypes = after.getProperty(JCR_SUPERTYPES);
        if (supertypes != null) {
            for (String value : supertypes.getValue(NAMES)) {
                if (!types.hasChildNode(value)) {
                    throw new CommitFailedException(
                            "Missing supertype " + value + " in " + path);
                }
            }
        }

        type.setProperty(JCR_PRIMARYTYPE, "oak:nodeType", NAME);
        type.removeNode("oak:namedPropertyDefinitions");
        type.removeNode("oak:residualPropertyDefinitions");
        type.removeNode("oak:namedChildNodeDefinitions");
        type.removeNode("oak:residualChildNodeDefinitions");

        // + jcr:propertyDefinition (nt:propertyDefinition)
        //   = nt:propertyDefinition protected sns
        // + jcr:childNodeDefinition (nt:childNodeDefinition)
        //   = nt:childNodeDefinition protected sns
        for (ChildNodeEntry entry : after.getChildNodeEntries()) {
            String childName = entry.getName();
            if (childName.startsWith(JCR_PROPERTYDEFINITION)) {
                processPropertyDefinition(type, entry.getNodeState());
            } else if (childName.startsWith(JCR_CHILDNODEDEFINITION)) {
                processChildNodeDefinition(
                        types, type, entry.getNodeState());
            }
        }
    }

    private void processPropertyDefinition(
            NodeBuilder type, NodeState definition)
            throws CommitFailedException {
        // - jcr:name (NAME) protected 
        PropertyState name = definition.getProperty(JCR_NAME);
        NodeBuilder definitions;
        if (name != null) {
            definitions = type.child("oak:namedPropertyDefinitions");
            definitions.setProperty(
                    JCR_PRIMARYTYPE, "oak:namedPropertyDefinitions", NAME);
            definitions = definitions.child(name.getValue(NAME));
        } else {
            definitions = type.child("oak:residualChildNodeDefinitions");
        }
        definitions.setProperty(
                JCR_PRIMARYTYPE, "oak:propertyDefinitions", NAME);

        // - jcr:requiredType (STRING) protected mandatory
        // < 'STRING', 'URI', 'BINARY', 'LONG', 'DOUBLE',
        //   'DECIMAL', 'BOOLEAN', 'DATE', 'NAME', 'PATH',
        //   'REFERENCE', 'WEAKREFERENCE', 'UNDEFINED'
        String key = "UNDEFINED";
        PropertyState requiredType = definition.getProperty(JCR_REQUIREDTYPE);
        if (requiredType != null) {
            key = requiredType.getValue(STRING);
        }

        // - jcr:multiple (BOOLEAN) protected mandatory
        PropertyState multiple = definition.getProperty(JCR_MULTIPLE);
        if (multiple != null && multiple.getValue(Type.BOOLEAN)) {
            key = key + "-ARRAY";
        }

        definitions.setNode(key, definition);
    }

    private void processChildNodeDefinition(
            NodeBuilder types, NodeBuilder type, NodeState definition)
            throws CommitFailedException {
        // - jcr:name (NAME) protected 
        PropertyState name = definition.getProperty(JCR_NAME);
        NodeBuilder definitions;
        if (name != null) {
            definitions = type.child("oak:namedPropertyDefinitions");
            definitions.setProperty(
                    JCR_PRIMARYTYPE, "oak:namedPropertyDefinitions", NAME);
            definitions = definitions.child(name.getValue(NAME));
        } else {
            definitions = type.child("oak:residualChildNodeDefinitions");
        }
        definitions.setProperty(
                JCR_PRIMARYTYPE, "oak:childNodeDefinitions", NAME);

        // - jcr:requiredPrimaryTypes (NAME)
        //   = 'nt:base' protected mandatory multiple
        PropertyState requiredTypes = definition.getProperty(JCR_REQUIREDTYPE);
        if (requiredTypes != null) {
            for (String key : requiredTypes.getValue(NAMES)) {
                if (!types.hasChildNode(key)) {
                    throw new CommitFailedException(
                            "Unknown required primary type " + key);
                } else if (!definitions.hasChildNode(key)) {
                    definitions.setNode(key, definition);
                }
            }
        }
    }

    /**
     * Updates the {@link #changedTypes} set to contain also all subtypes
     * that may have been affected by the content changes even if they haven't
     * been directly modified.
     *
     * @param types {@code /jcr:system/jcr:nodeTypes} after the changes
     */
    private void findAllAffectedTypes(NodeState types) {
        Queue<String> queue = Queues.newArrayDeque(changedTypes);
        while (!queue.isEmpty()) {
            String name = queue.remove();

            // TODO: We should be able to do this with just one pass
            for (ChildNodeEntry entry : types.getChildNodeEntries()) {
                NodeState type = entry.getNodeState();
                PropertyState supertypes = type.getProperty(JCR_SUPERTYPES);
                if (supertypes != null) {
                    for (String superName : supertypes.getValue(NAMES)) {
                        if (name.equals(superName)) {
                            if (!changedTypes.add(entry.getName())) {
                                queue.add(entry.getName());
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Verifies that none of the remaining node types still references
     * one of the removed types.
     *
     * @param types {@code /jcr:system/jcr:nodeTypes} after the changes
     * @throws CommitFailedException if a removed type is still referenced
     */
    private void checkTypeReferencesToRemovedTypes(NodeState types)
            throws CommitFailedException {
        for (ChildNodeEntry entry : types.getChildNodeEntries()) {
            NodeState type = entry.getNodeState();

            // Are there any supertype references to removed types?
            PropertyState supertypes = type.getProperty(JCR_SUPERTYPES);
            if (supertypes != null) {
                for (String superName : supertypes.getValue(NAMES)) {
                    if (removedTypes.contains(superName)) {
                        throw new CommitFailedException(
                                "Removed type " + superName
                                + " is still referenced as a supertype of "
                                + entry.getName());
                    }
                }
            }

            // Are there any child node definition references to removed types?
            for (ChildNodeEntry childEntry : types.getChildNodeEntries()) {
                String childName = childEntry.getName();
                if (childName.startsWith(JCR_CHILDNODEDEFINITION)) {
                    NodeState definition = childEntry.getNodeState();
                    PropertyState requiredTypes =
                            definition.getProperty(JCR_REQUIREDTYPE);
                    if (requiredTypes != null) {
                        for (String required : requiredTypes.getValue(NAMES)) {
                            if (removedTypes.contains(required)) {
                                throw new CommitFailedException(
                                        "Removed type " + required
                                        + " is still referenced as a required "
                                        + " primary child node type in "
                                        + entry.getName());
                            }
                        }
                    }
                }
            }
        }
    }

    //------------------------------------------------------------< Editor >--

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        if (!removedTypes.isEmpty()) {
            checkTypeReferencesToRemovedTypes(after);
        }

        if (!changedTypes.isEmpty()) {
            findAllAffectedTypes(after);
        }

        if (!changedTypes.isEmpty() || !removedTypes.isEmpty()) {
            // TODO: Find and re-validate any nodes in the repository that
            // refer to any of the changed (or removed) node types.
        }
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        validateAndCompile(name, after);
        return null;
    }

    @Override
    public Validator childNodeChanged(
            String name, NodeState before, NodeState after)
            throws CommitFailedException {
        validateAndCompile(name, after);
        changedTypes.add(name);
        return null;
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        removedTypes.add(name);
        return null;
    }

}
