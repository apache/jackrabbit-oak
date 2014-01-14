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

import static java.util.Arrays.asList;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import com.google.common.collect.ImmutableMap;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.privilege.ImmutablePrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.util.NodeUtil;

/**
 * PrivilegeDefinitionWriter is responsible for writing privilege definitions
 * to the repository without applying any validation checks.
 */
class PrivilegeDefinitionWriter implements PrivilegeConstants {

    /**
     * The internal names of all built-in privileges that are not aggregates.
     */
    private static final String[] NON_AGGR_PRIVILEGES = new String[]{
            REP_READ_NODES, REP_READ_PROPERTIES,
            REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES, REP_REMOVE_PROPERTIES,
            JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE,
            JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL, JCR_NODE_TYPE_MANAGEMENT,
            JCR_VERSION_MANAGEMENT, JCR_LOCK_MANAGEMENT, JCR_LIFECYCLE_MANAGEMENT,
            JCR_RETENTION_MANAGEMENT, JCR_WORKSPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT,
            JCR_NAMESPACE_MANAGEMENT, REP_PRIVILEGE_MANAGEMENT, REP_USER_MANAGEMENT};

    /**
     * The internal names and aggregation definition of all built-in privileges
     * that are aggregates (except for jcr:all).
     */
    private static final Map<String, String[]> AGGREGATE_PRIVILEGES = ImmutableMap.of(
            JCR_READ, new String[]{REP_READ_NODES, REP_READ_PROPERTIES},
            JCR_MODIFY_PROPERTIES, new String[]{REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES, REP_REMOVE_PROPERTIES},
            JCR_WRITE, new String[]{JCR_MODIFY_PROPERTIES, JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE},
            REP_WRITE, new String[]{JCR_WRITE, JCR_NODE_TYPE_MANAGEMENT});

    private final Root root;
    private final PrivilegeBitsProvider bitsMgr;

    private PrivilegeBits next;

    PrivilegeDefinitionWriter(Root root) {
        this.root = root;
        this.bitsMgr = new PrivilegeBitsProvider(root);
        Tree privilegesTree = bitsMgr.getPrivilegesTree();
        if (privilegesTree.exists() && privilegesTree.hasProperty(REP_NEXT)) {
            next = PrivilegeBits.getInstance(privilegesTree);
        } else {
            next = PrivilegeBits.NEXT_AFTER_BUILT_INS;
        }
    }

    /**
     * Write the given privilege definition to the repository content.
     *
     * @param definition The new privilege definition.
     * @throws RepositoryException If the definition can't be written.
     */
    void writeDefinition(PrivilegeDefinition definition) throws RepositoryException {
        writeDefinitions(Collections.singleton(definition));
    }

    /**
     * Create the built-in privilege definitions during repository setup.
     *
     * @throws RepositoryException If an error occurs.
     */
    void writeBuiltInDefinitions() throws RepositoryException {
        writeDefinitions(getBuiltInDefinitions());
    }

    //--------------------------------------------------------------------------
    @Nonnull
    private PrivilegeBits getNext() {
        return next;
    }

    @Nonnull
    private PrivilegeBits next() {
        PrivilegeBits bits = next;
        next = bits.nextBits();
        return bits;
    }

    /**
     * @param definitions
     * @throws RepositoryException
     */
    private void writeDefinitions(Iterable<PrivilegeDefinition> definitions) throws RepositoryException {
        try {
            // make sure the privileges path is defined
            Tree privilegesTree = root.getTree(PRIVILEGES_PATH);
            if (!privilegesTree.exists()) {
                throw new RepositoryException("Privilege store does not exist.");
            }
            NodeUtil privilegesNode = new NodeUtil(privilegesTree);
            for (PrivilegeDefinition definition : definitions) {
                if (privilegesNode.hasChild(definition.getName())) {
                    throw new RepositoryException("Privilege definition with name '" + definition.getName() + "' already exists.");
                }
                writePrivilegeNode(privilegesNode, definition);
            }
            /*
            update the property storing the next privilege bits with the
            privileges root tree. this is a cheap way to detect collisions that
            may arise from concurrent registration of custom privileges.
            */
            getNext().writeTo(privilegesTree);

            // delegate validation to the commit validation (see above)
            root.commit();

        } catch (CommitFailedException e) {
            throw e.asRepositoryException();
        }
    }

    private void writePrivilegeNode(NodeUtil privilegesNode, PrivilegeDefinition definition) throws RepositoryException {
        String name = definition.getName();
        NodeUtil privNode = privilegesNode.addChild(name, NT_REP_PRIVILEGE);
        if (definition.isAbstract()) {
            privNode.setBoolean(REP_IS_ABSTRACT, true);
        }
        String[] declAggrNames = definition.getDeclaredAggregateNames().toArray(new String[definition.getDeclaredAggregateNames().size()]);
        boolean isAggregate = declAggrNames.length > 0;
        if (isAggregate) {
            privNode.setNames(REP_AGGREGATES, declAggrNames);
        }

        PrivilegeBits bits;
        if (PrivilegeBits.BUILT_IN.containsKey(name)) {
            bits = PrivilegeBits.BUILT_IN.get(name);
        } else if (isAggregate) {
            bits = bitsMgr.getBits(declAggrNames);
        } else {
            bits = next();
        }
        bits.writeTo(privNode.getTree());
    }

    private static Collection<PrivilegeDefinition> getBuiltInDefinitions() {
        Map<String, PrivilegeDefinition> definitions = new LinkedHashMap<String, PrivilegeDefinition>();
        for (String privilegeName : NON_AGGR_PRIVILEGES) {
            PrivilegeDefinition def = new ImmutablePrivilegeDefinition(privilegeName, false, null);
            definitions.put(privilegeName, def);
        }
        for (String privilegeName : AGGREGATE_PRIVILEGES.keySet()) {
            PrivilegeDefinition def = new ImmutablePrivilegeDefinition(privilegeName, false, asList(AGGREGATE_PRIVILEGES.get(privilegeName)));
            definitions.put(privilegeName, def);
        }
        PrivilegeDefinition all = new ImmutablePrivilegeDefinition(JCR_ALL, false, definitions.keySet());
        definitions.put(JCR_ALL, all);
        return definitions.values();
    }
}