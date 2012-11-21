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
package org.apache.jackrabbit.oak.security.privilege;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.Text;

/**
 * Validator implementation that is responsible for validating any modifications
 * made to privileges stored in the repository.
 */
class PrivilegeValidator implements PrivilegeConstants, Validator {

    private final Map<String, PrivilegeDefinition> definitions;
    private final PrivilegeDefinitionReaderImpl reader;

    PrivilegeValidator(Tree rootBefore) {
        Tree privilegesBefore = null;
        Tree system = rootBefore.getChild(JcrConstants.JCR_SYSTEM);
        if (system != null) {
            privilegesBefore = system.getChild(REP_PRIVILEGES);
        }

        if (privilegesBefore != null) {
            reader = new PrivilegeDefinitionReaderImpl(privilegesBefore);
            definitions = reader.readDefinitions();
        } else {
            reader = null;
            definitions = null;
        }
    }

    //----------------------------------------------------------< Validator >---
    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        // no-op
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        throw new CommitFailedException("Attempt to modify existing privilege definition.");
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        throw new CommitFailedException("Attempt to modify existing privilege definition.");
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        checkInitialized();

        // the following characteristics are expected to be validated elsewhere:
        // - permission to allow privilege registration -> permission validator.
        // - name collisions (-> delegated to NodeTypeValidator since sms are not allowed)
        // - name must be valid (-> delegated to NameValidator)

        // name may not contain reserved namespace prefix
        if (NamespaceConstants.RESERVED_PREFIXES.contains(Text.getNamespacePrefix(name))) {
            String msg = "Failed to register custom privilege: Definition uses reserved namespace: " + name;
            throw new CommitFailedException(new RepositoryException(msg));
        }

        // primary node type name must be rep:privilege
        Tree tree = new ReadOnlyTree(null, name, after);
        PropertyState primaryType = tree.getProperty(JcrConstants.JCR_PRIMARYTYPE);
        if (primaryType == null || !NT_REP_PRIVILEGE.equals(primaryType.getValue(Type.STRING))) {
            throw new CommitFailedException("Privilege definition must have primary node type set to rep:privilege");
        }

        // additional validation of the definition
        PrivilegeDefinition def = reader.readDefinition(tree);
        validateDefinition(def);

        // privilege definitions may not have child nodes.
        return null;
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        throw new CommitFailedException("Attempt to modify existing privilege definition " + name);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        throw new CommitFailedException("Attempt to un-register privilege " + name);
    }

    //------------------------------------------------------------< private >---
    /**
     * Validation of the privilege definition including the following steps:
     *
     * - all aggregates must have been registered before
     * - no existing privilege defines the same aggregation
     * - no cyclic aggregation
     *
     * @param definition The new privilege definition to validate.
     * @throws org.apache.jackrabbit.oak.api.CommitFailedException If any of
     * the checks listed above fails.
     */
    private void validateDefinition(PrivilegeDefinition definition) throws CommitFailedException {
        Set<String> declaredNames = definition.getDeclaredAggregateNames();
        if (declaredNames.isEmpty()) {
            return;
        }

        if (declaredNames.size() == 1) {
            throw new CommitFailedException("Singular aggregation is equivalent to existing privilege.");
        }

        for (String aggrName : declaredNames) {
            // aggregated privilege not registered
            if (!definitions.containsKey(aggrName)) {
                throw new CommitFailedException("Declared aggregate '"+ aggrName +"' is not a registered privilege.");
            }

            // check for circular aggregation
            if (isCircularAggregation(definition.getName(), aggrName)) {
                String msg = "Detected circular aggregation within custom privilege caused by " + aggrName;
                throw new CommitFailedException(msg);
            }
        }

        Set<String> aggregateNames = resolveAggregates(declaredNames);
        for (PrivilegeDefinition existing : definitions.values()) {
            Set<String> existingDeclared = existing.getDeclaredAggregateNames();
            if (existingDeclared.isEmpty()) {
                continue;
            }

            // test for exact same aggregation or aggregation with the same net effect
            if (declaredNames.equals(existingDeclared) || aggregateNames.equals(resolveAggregates(existingDeclared))) {
                String msg = "Custom aggregate privilege '" + definition.getName() + "' is already covered by '" + existing.getName() + '\'';
                throw new CommitFailedException(msg);
            }
        }
    }

    private boolean isCircularAggregation(String privilegeName, String aggregateName) {
        if (privilegeName.equals(aggregateName)) {
            return true;
        }

        PrivilegeDefinition aggrPriv = definitions.get(aggregateName);
        if (aggrPriv.getDeclaredAggregateNames().isEmpty()) {
            return false;
        } else {
            boolean isCircular = false;
            for (String name : aggrPriv.getDeclaredAggregateNames()) {
                if (privilegeName.equals(name)) {
                    return true;
                }
                if (definitions.containsKey(name)) {
                    isCircular = isCircularAggregation(privilegeName, name);
                }
            }
            return isCircular;
        }
    }

    private Set<String> resolveAggregates(Set<String> declared) throws CommitFailedException {
        Set<String> aggregateNames = new HashSet<String>();
        for (String name : declared) {
            PrivilegeDefinition d = definitions.get(name);
            if (d == null) {
                throw new CommitFailedException("Invalid declared aggregate name " + name + ": Unknown privilege.");
            }

            Set<String> names = d.getDeclaredAggregateNames();
            if (names.isEmpty()) {
                aggregateNames.add(name);
            } else {
                aggregateNames.addAll(resolveAggregates(names));
            }
        }
        return aggregateNames;
    }

    private void checkInitialized() throws CommitFailedException {
        if (reader == null || definitions == null) {
            throw new CommitFailedException(new IllegalStateException("Mandatory privileges root is missing."));
        }
    }
}