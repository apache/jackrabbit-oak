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
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeUtil;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.util.Text;

import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;

/**
 * Validator implementation that is responsible for validating any modifications
 * made to privileges stored in the repository.
 */
class PrivilegeValidator extends DefaultValidator implements PrivilegeConstants {

    private final Root rootBefore;
    private final Root rootAfter;
    private final PrivilegeBitsProvider bitsProvider;

    PrivilegeValidator(Root before, Root after) {
        rootBefore = before;
        rootAfter = after;
        bitsProvider = new PrivilegeBitsProvider(rootBefore);
    }

    //----------------------------------------------------------< Validator >---
    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        // no-op
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        if (REP_NEXT.equals(before.getName())) {
            validateNext(PrivilegeBits.getInstance(getPrivilegesTree(rootBefore).getProperty(REP_NEXT)));
        } else {
            throw new CommitFailedException(CONSTRAINT, 45, "Attempt to modify existing privilege definition.");
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        throw new CommitFailedException(CONSTRAINT, 46, "Attempt to modify existing privilege definition.");
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        if (isPrivilegeDefinition(after)) {
            // make sure privileges have been initialized before
            getPrivilegesTree(rootBefore);

            // the following characteristics are expected to be validated elsewhere:
            // - permission to allow privilege registration -> permission validator.
            // - name collisions (-> delegated to NodeTypeValidator since sms are not allowed)
            // - name must be valid (-> delegated to NameValidator)

            // name may not contain reserved namespace prefix
            if (NamespaceConstants.RESERVED_PREFIXES.contains(Text.getNamespacePrefix(name))) {
                String msg = "Failed to register custom privilege: Definition uses reserved namespace: " + name;
                throw new CommitFailedException("Privilege", 1, msg);
            }

            // validate the definition
            Tree tree = new ImmutableTree(ImmutableTree.ParentProvider.UNSUPPORTED, name, after);
            validateDefinition(tree);
        }

        // privilege definitions may not have child nodes (or another type of nodes
        // that is not handled by this validator anyway).
        return null;
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        if (isPrivilegeDefinition(before) && !before.equals(after)) {
            throw new CommitFailedException(CONSTRAINT, 41, "Attempt to modify existing privilege definition " + name);
        } else {
            // not handled by this validator
            return null;
        }
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        if (isPrivilegeDefinition(before)) {
            throw new CommitFailedException(CONSTRAINT, 42, "Attempt to un-register privilege " + name);
        }  else {
            // not handled by this validator
            return null;
        }
    }

    //------------------------------------------------------------< private >---
    private void validateNext(PrivilegeBits bits) throws CommitFailedException {
        PrivilegeBits next = PrivilegeBits.getInstance(getPrivilegesTree(rootAfter).getProperty(REP_NEXT));
        if (!next.equals(bits.nextBits())) {
            throw new CommitFailedException(CONSTRAINT, 43, "Next bits not updated");
        }
    }

    @Nonnull
    private Tree getPrivilegesTree(Root root) throws CommitFailedException {
        Tree privilegesTree = root.getTree(PRIVILEGES_PATH);
        if (!privilegesTree.exists()) {
            throw new CommitFailedException(CONSTRAINT, 44, "Privilege store not initialized.");
        }
        return privilegesTree;
    }

    /**
     * Validation of the privilege definition including the following steps:
     * <p>
     * - privilege bits must not collide with an existing privilege
     * - next bits must have been adjusted in case of a non-aggregate privilege
     * - all aggregates must have been registered before
     * - no existing privilege defines the same aggregation
     * - no cyclic aggregation
     *
     * @param definitionTree The new privilege definition tree to validate.
     * @throws org.apache.jackrabbit.oak.api.CommitFailedException
     *          If any of
     *          the checks listed above fails.
     */
    private void validateDefinition(Tree definitionTree) throws CommitFailedException {
        PrivilegeBits newBits = PrivilegeBits.getInstance(definitionTree);
        if (newBits.isEmpty()) {
            throw new CommitFailedException(CONSTRAINT, 48, "PrivilegeBits are missing.");
        }

        Set<String> privNames = bitsProvider.getPrivilegeNames(newBits);
        PrivilegeDefinition definition = PrivilegeUtil.readDefinition(definitionTree);
        Set<String> declaredNames = definition.getDeclaredAggregateNames();

        // non-aggregate privilege
        if (declaredNames.isEmpty()) {
            if (!privNames.isEmpty()) {
                throw new CommitFailedException(CONSTRAINT, 49, "PrivilegeBits already in used.");
            }
            validateNext(newBits);
            return;
        }

        // aggregation of a single privilege
        if (declaredNames.size() == 1) {
            throw new CommitFailedException(CONSTRAINT, 50, "Singular aggregation is equivalent to existing privilege.");
        }

        // aggregation of >1 privileges
        Map<String, PrivilegeDefinition> definitions = new PrivilegeDefinitionReader(rootBefore).readDefinitions();
        for (String aggrName : declaredNames) {
            // aggregated privilege not registered
            if (!definitions.containsKey(aggrName)) {
                throw new CommitFailedException(CONSTRAINT, 51, "Declared aggregate '" + aggrName + "' is not a registered privilege.");
            }

            // check for circular aggregation
            if (isCircularAggregation(definition.getName(), aggrName, definitions)) {
                String msg = "Detected circular aggregation within custom privilege caused by " + aggrName;
                throw new CommitFailedException(CONSTRAINT, 52, msg);
            }
        }

        Set<String> aggregateNames = resolveAggregates(declaredNames, definitions);
        for (PrivilegeDefinition existing : definitions.values()) {
            Set<String> existingDeclared = existing.getDeclaredAggregateNames();
            if (existingDeclared.isEmpty()) {
                continue;
            }

            // test for exact same aggregation or aggregation with the same net effect
            if (declaredNames.equals(existingDeclared) || aggregateNames.equals(resolveAggregates(existingDeclared, definitions))) {
                String msg = "Custom aggregate privilege '" + definition.getName() + "' is already covered by '" + existing.getName() + '\'';
                throw new CommitFailedException(CONSTRAINT, 53, msg);
            }
        }

        PrivilegeBits aggrBits = bitsProvider.getBits(declaredNames.toArray(new String[declaredNames.size()]));
        if (!newBits.equals(aggrBits)) {
            throw new CommitFailedException(CONSTRAINT, 53, "Invalid privilege bits for aggregated privilege definition.");
        }
    }

    private static boolean isCircularAggregation(String privilegeName, String aggregateName,
                                                 Map<String, PrivilegeDefinition> definitions) {
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
                    isCircular = isCircularAggregation(privilegeName, name, definitions);
                }
            }
            return isCircular;
        }
    }

    private static Set<String> resolveAggregates(Set<String> declared, Map<String, PrivilegeDefinition> definitions) throws CommitFailedException {
        Set<String> aggregateNames = new HashSet<String>();
        for (String name : declared) {
            PrivilegeDefinition d = definitions.get(name);
            if (d == null) {
                throw new CommitFailedException(CONSTRAINT, 47, "Invalid declared aggregate name " + name + ": Unknown privilege.");
            }

            Set<String> names = d.getDeclaredAggregateNames();
            if (names.isEmpty()) {
                aggregateNames.add(name);
            } else {
                aggregateNames.addAll(resolveAggregates(names, definitions));
            }
        }
        return aggregateNames;
    }

    private static boolean isPrivilegeDefinition(@Nonnull NodeState state) {
        return NT_REP_PRIVILEGE.equals(NodeStateUtils.getPrimaryTypeName(state));
    }
}