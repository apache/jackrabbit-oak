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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
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
 * FIXME: Privilege registation should result in Session#refresh in order to have the new privilege also exposed in the content.
 */
public class PrivilegeRegistry implements PrivilegeProvider, PrivilegeConstants {

    private static final Map<String, String[]> AGGREGATE_PRIVILEGES = new HashMap<String,String[]>();
    static {
        AGGREGATE_PRIVILEGES.put(JCR_READ, AGGR_JCR_READ);
        AGGREGATE_PRIVILEGES.put(JCR_MODIFY_PROPERTIES, AGGR_JCR_MODIFY_PROPERTIES);
        AGGREGATE_PRIVILEGES.put(JCR_WRITE, AGGR_JCR_WRITE);
        AGGREGATE_PRIVILEGES.put(REP_WRITE, AGGR_REP_WRITE);
    }

    private final ContentSession contentSession;
    private final Root root;

    private final Map<String, PrivilegeDefinition> definitions;

    public PrivilegeRegistry(ContentSession contentSession, Root root) {
        this.contentSession = contentSession;
        this.root = root;
        this.definitions = readDefinitions(root);
    }

    static Map<String, PrivilegeDefinition> getAllDefinitions(PrivilegeDefinitionReader reader) {
        Map<String, PrivilegeDefinition> definitions = new HashMap<String, PrivilegeDefinition>();
        for (String privilegeName : NON_AGGR_PRIVILEGES) {
            PrivilegeDefinition def = new PrivilegeDefinitionImpl(privilegeName, false);
            definitions.put(privilegeName, def);
        }

        for (String privilegeName : AGGREGATE_PRIVILEGES.keySet()) {
            PrivilegeDefinition def = new PrivilegeDefinitionImpl(privilegeName, false, AGGREGATE_PRIVILEGES.get(privilegeName));
            definitions.put(privilegeName, def);
        }

        // add custom definitions
        definitions.putAll(reader.readDefinitions());
        updateJcrAllPrivilege(definitions);
        return definitions;
    }

    private Map<String, PrivilegeDefinition> readDefinitions(Root root) {
        return getAllDefinitions(new PrivilegeDefinitionReader(root));
    }

    private static void updateJcrAllPrivilege(Map<String, PrivilegeDefinition> definitions) {
        Map<String, PrivilegeDefinition> m = new HashMap<String, PrivilegeDefinition>(definitions);
        m.remove(JCR_ALL);
        definitions.put(JCR_ALL, new PrivilegeDefinitionImpl(JCR_ALL, false, m.keySet()));
    }

    //--------------------------------------------------< PrivilegeProvider >---
    @Override
    public void refresh() {
        // re-read the definitions (TODO: evaluate if it was better to always read privileges on demand only.)
        definitions.putAll(readDefinitions(root));
    }

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
        Root latestRoot = contentSession.getLatestRoot();
        try {
            // make sure the privileges path is defined
            Tree privilegesTree = latestRoot.getTree(PRIVILEGES_PATH);
            if (privilegesTree == null) {
                throw new RepositoryException("Repository doesn't contain node " + PRIVILEGES_PATH);
            }

            NodeUtil privilegesNode = new NodeUtil(privilegesTree, latestRoot.getValueFactory());
            writeDefinition(privilegesNode, toRegister);

            // delegate validation to the commit validation (see above)
            latestRoot.commit();

        } catch (CommitFailedException e) {
            Throwable t = e.getCause();
            if (t instanceof RepositoryException) {
                throw (RepositoryException) t;
            } else {
                throw new RepositoryException(e.getMessage());
            }
        }

        // TODO: should be covered by refresh instead
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