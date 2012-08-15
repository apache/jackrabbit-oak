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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.DefaultConflictHandler;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.util.Text;

/**
 * PrivilegeRegistry... TODO
 *
 *
 * TODO: define if/how built-in privileges are reflected in the mk
 * TODO: define if custom privileges are read with editing content session (thus enforcing read permissions)
 *
 * FIXME: Session#refresh should refresh privileges exposed
 */
public class PrivilegeRegistry implements PrivilegeProvider, PrivilegeConstants, Validator {

    private static final String[] SIMPLE_PRIVILEGES = new String[] {
            JCR_READ, REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES, REP_REMOVE_PROPERTIES,
            JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE,
            JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL, JCR_NODE_TYPE_MANAGEMENT,
            JCR_VERSION_MANAGEMENT, JCR_LOCK_MANAGEMENT, JCR_LIFECYCLE_MANAGEMENT,
            JCR_RETENTION_MANAGEMENT, JCR_WORKSPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT,
            JCR_NAMESPACE_MANAGEMENT, REP_PRIVILEGE_MANAGEMENT};

    private static final Map<String, String[]> AGGREGATE_PRIVILEGES = new HashMap<String,String[]>();
    static {
        AGGREGATE_PRIVILEGES.put(JCR_MODIFY_PROPERTIES,
                new String[] {REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES, REP_REMOVE_PROPERTIES});
        AGGREGATE_PRIVILEGES.put(JCR_WRITE,
                new String[] {JCR_MODIFY_PROPERTIES, JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE});
        AGGREGATE_PRIVILEGES.put(REP_WRITE,
                new String[] {JCR_WRITE, JCR_NODE_TYPE_MANAGEMENT});
    }

    private final ContentSession contentSession;
    private final PrivilegeDefinitionReader reader;

    private final Map<String, PrivilegeDefinition> definitions = new HashMap<String, PrivilegeDefinition>();

    public PrivilegeRegistry(ContentSession contentSession) throws RepositoryException {

        this.contentSession = contentSession;

        for (String privilegeName : SIMPLE_PRIVILEGES) {
            PrivilegeDefinition def = new PrivilegeDefinitionImpl(privilegeName, false);
            definitions.put(privilegeName, def);
        }

        for (String privilegeName : AGGREGATE_PRIVILEGES.keySet()) {
            PrivilegeDefinition def = new PrivilegeDefinitionImpl(privilegeName, false, AGGREGATE_PRIVILEGES.get(privilegeName));
            definitions.put(privilegeName, def);
        }

        this.reader = new PrivilegeDefinitionReader(contentSession);
        definitions.putAll(reader.readDefinitions());
        updateJcrAllPrivilege();
    }

    //--------------------------------------------------< PrivilegeProvider >---
    @Nonnull
    @Override
    public PrivilegeDefinition[] getPrivilegeDefinitions() {
        return definitions.values().toArray(new PrivilegeDefinition[definitions.size()]);
    }

    @Override
    public PrivilegeDefinition getPrivilegeDefinition(String name) {
        return definitions.get(name);
    }

    @Override
    public PrivilegeDefinition registerDefinition(
            final String privilegeName, final boolean isAbstract,
            final Set<String> declaredAggregateNames)
            throws RepositoryException {

        PrivilegeDefinition definition = new PrivilegeDefinitionImpl(privilegeName, isAbstract, declaredAggregateNames);
        internalRegisterDefinitions(definition);
        return definition;
    }

    public void registerDefinition(PrivilegeDefinition definition) throws RepositoryException {
        PrivilegeDefinition toRegister;
        if (definition instanceof PrivilegeDefinitionImpl) {
            toRegister = definition;
        } else {
            toRegister = new PrivilegeDefinitionImpl(definition.getName(), definition.isAbstract(), definition.getDeclaredAggregateNames());
        }
        internalRegisterDefinitions(toRegister);
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
        if (primaryType == null || !NT_REP_PRIVILEGE.equals(primaryType.getValue().getString())) {
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

    private void updateJcrAllPrivilege() {
        // TODO: add proper implementation taking custom privileges into account.
        definitions.put(JCR_ALL, new PrivilegeDefinitionImpl(JCR_ALL, false,
                JCR_READ, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL,
                JCR_VERSION_MANAGEMENT, JCR_LOCK_MANAGEMENT, JCR_LIFECYCLE_MANAGEMENT,
                JCR_RETENTION_MANAGEMENT, JCR_WORKSPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT,
                JCR_NAMESPACE_MANAGEMENT, REP_PRIVILEGE_MANAGEMENT, REP_WRITE));
    }

    private void internalRegisterDefinitions(PrivilegeDefinition toRegister) throws RepositoryException {
        CoreValueFactory vf = contentSession.getCoreValueFactory();
        Root root = contentSession.getCurrentRoot();

        try {
            // make sure the privileges path is defined
            Tree privilegesTree = root.getTree(PRIVILEGES_PATH);
            if (privilegesTree == null) {
                throw new RepositoryException("Repository doesn't contain node " + PRIVILEGES_PATH);
            }

            NodeUtil privilegesNode = new NodeUtil(privilegesTree, contentSession);
            writeDefinition(privilegesNode, toRegister);

            // delegate validation to the commit validation (see above)
            root.commit(DefaultConflictHandler.OURS);

        } catch (CommitFailedException e) {
            throw new RepositoryException(e.getMessage());
        }

        definitions.put(toRegister.getName(), toRegister);
        updateJcrAllPrivilege();
    }

    private void writeDefinition(NodeUtil privilegesNode, PrivilegeDefinition definition) {
        NodeUtil privNode = privilegesNode.addChild(definition.getName(), NT_REP_PRIVILEGE);
        if (definition.isAbstract()) {
            privNode.setBoolean(REP_IS_ABSTRACT, true);
        }
        Set<String> declAggrNames = definition.getDeclaredAggregateNames();
        if (!declAggrNames.isEmpty()) {
            String[] names = definition.getDeclaredAggregateNames().toArray(new String[declAggrNames.size()]);
            privNode.setNames(REP_AGGREGATES, names);
        }
    }

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
        Set<String> aggrNames = definition.getDeclaredAggregateNames();
        if (aggrNames.isEmpty()) {
            return;
        }

        for (String aggrName : aggrNames) {
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

        for (PrivilegeDefinition existing : definitions.values()) {
            if (aggrNames.equals(existing.getDeclaredAggregateNames())) {
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
}