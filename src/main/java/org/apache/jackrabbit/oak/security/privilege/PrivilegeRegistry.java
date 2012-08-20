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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.DefaultConflictHandler;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeProvider;
import org.apache.jackrabbit.oak.util.NodeUtil;

/**
 * PrivilegeRegistry... TODO
 *
 *
 * TODO: define if/how built-in privileges are reflected in the mk
 * TODO: define if custom privileges are read with editing content session (thus enforcing read permissions)
 *
 * FIXME: Session#refresh should refresh privileges exposed
 * FIXME: Proper implementation for JCR_ALL privilege containing all custom privileges.
 */
public class PrivilegeRegistry implements PrivilegeProvider, PrivilegeConstants {

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

    private final Map<String, PrivilegeDefinition> definitions;

    public PrivilegeRegistry(ContentSession contentSession) {
        this.contentSession = contentSession;
        this.definitions = getAllDefinitions(new PrivilegeDefinitionReader(contentSession));
    }

    static Map<String, PrivilegeDefinition> getAllDefinitions(PrivilegeDefinitionReader reader) {
        Map<String, PrivilegeDefinition> definitions = new HashMap<String, PrivilegeDefinition>();
        for (String privilegeName : SIMPLE_PRIVILEGES) {
            PrivilegeDefinition def = new PrivilegeDefinitionImpl(privilegeName, false);
            definitions.put(privilegeName, def);
        }

        for (String privilegeName : AGGREGATE_PRIVILEGES.keySet()) {
            PrivilegeDefinition def = new PrivilegeDefinitionImpl(privilegeName, false, AGGREGATE_PRIVILEGES.get(privilegeName));
            definitions.put(privilegeName, def);
        }

        definitions.putAll(reader.readDefinitions());
        updateJcrAllPrivilege(definitions);
        return definitions;
    }

    private static void updateJcrAllPrivilege(Map<String, PrivilegeDefinition> definitions) {
        // TODO: add proper implementation taking custom privileges into account.
        Set<String> declaredAggregateNames = new HashSet<String>();
        declaredAggregateNames.add(JCR_READ);
        declaredAggregateNames.add(JCR_READ_ACCESS_CONTROL);
        declaredAggregateNames.add(JCR_MODIFY_ACCESS_CONTROL);
        declaredAggregateNames.add(JCR_VERSION_MANAGEMENT);
        declaredAggregateNames.add(JCR_LOCK_MANAGEMENT);
        declaredAggregateNames.add(JCR_LIFECYCLE_MANAGEMENT);
        declaredAggregateNames.add(JCR_RETENTION_MANAGEMENT);
        declaredAggregateNames.add(JCR_WORKSPACE_MANAGEMENT);
        declaredAggregateNames.add(JCR_NODE_TYPE_DEFINITION_MANAGEMENT);
        declaredAggregateNames.add(JCR_NAMESPACE_MANAGEMENT);
        declaredAggregateNames.add(REP_PRIVILEGE_MANAGEMENT);
        declaredAggregateNames.add(REP_WRITE);

        definitions.put(JCR_ALL, new PrivilegeDefinitionImpl(JCR_ALL, false, declaredAggregateNames));
    }

    void registerDefinitions(PrivilegeDefinition[] definitions) throws RepositoryException {
        for (PrivilegeDefinition definition : definitions) {
            PrivilegeDefinition toRegister;
            if (definition instanceof PrivilegeDefinitionImpl) {
                toRegister = definition;
            } else {
                toRegister = new PrivilegeDefinitionImpl(definition.getName(), definition.isAbstract(), definition.getDeclaredAggregateNames());
            }
            internalRegisterDefinitions(toRegister);
        }
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

    //------------------------------------------------------------< private >---

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
            Throwable t = e.getCause();
            if (t instanceof RepositoryException) {
                throw (RepositoryException) t;
            } else {
                throw new RepositoryException(e.getMessage());
            }
        }

        definitions.put(toRegister.getName(), toRegister);
        updateJcrAllPrivilege(definitions);
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
}