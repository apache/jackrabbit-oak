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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.privilege.ImmutablePrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.jetbrains.annotations.NotNull;

import static java.util.Arrays.asList;

/**
 * PrivilegeDefinitionWriter is responsible for writing privilege definitions
 * to the repository without applying any validation checks.
 */
class PrivilegeDefinitionWriter implements PrivilegeConstants {

    private final Root root;
    private final PrivilegeBitsProvider bitsMgr;

    private PrivilegeBits next;

    PrivilegeDefinitionWriter(@NotNull Root root) {
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
    void writeDefinition(@NotNull PrivilegeDefinition definition) throws RepositoryException {
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
    @NotNull
    private PrivilegeBits getNext() {
        return next;
    }

    @NotNull
    private PrivilegeBits next() {
        PrivilegeBits bits = next;
        next = bits.nextBits();
        return bits;
    }

    /**
     * @param definitions The privilege definitions to write to the repository.
     * @throws RepositoryException If the privilege store is missing or if there is a privilege registered with the same name.
     */
    private void writeDefinitions(@NotNull Iterable<PrivilegeDefinition> definitions) throws RepositoryException {
        try {
            // make sure the privileges path is defined
            Tree privilegesTree = root.getTree(PRIVILEGES_PATH);
            if (!privilegesTree.exists()) {
                throw new RepositoryException("Privilege store does not exist.");
            }
            for (PrivilegeDefinition definition : definitions) {
                if (privilegesTree.hasChild(definition.getName())) {
                    throw new RepositoryException("Privilege definition with name '" + definition.getName() + "' already exists.");
                }
                writePrivilegeNode(privilegesTree, definition);
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

    private void writePrivilegeNode(@NotNull Tree privilegesTree, @NotNull PrivilegeDefinition definition) throws RepositoryException {
        String name = definition.getName();
        Tree privNode = TreeUtil.addChild(privilegesTree, name, NT_REP_PRIVILEGE);
        if (definition.isAbstract()) {
            privNode.setProperty(REP_IS_ABSTRACT, true);
        }
        Set<String> declAggrNames = definition.getDeclaredAggregateNames();
        boolean isAggregate = !declAggrNames.isEmpty();
        if (isAggregate) {
            privNode.setProperty(REP_AGGREGATES, declAggrNames, Type.NAMES);
        }

        PrivilegeBits bits;
        if (PrivilegeBits.BUILT_IN.containsKey(name)) {
            bits = PrivilegeBits.BUILT_IN.get(name);
        } else if (isAggregate) {
            bits = bitsMgr.getBits(declAggrNames);
            if (bits.isEmpty()) {
                throw new RepositoryException("Illegal aggregation of non-exising privileges on '" + name + "'.");
            }
        } else {
            bits = next();
        }
        bits.writeTo(privNode);
    }

    @NotNull
    private static Collection<PrivilegeDefinition> getBuiltInDefinitions() {
        Map<String, PrivilegeDefinition> definitions = new LinkedHashMap<>();
        NON_AGGREGATE_PRIVILEGES.forEach((privilegeName) -> {
            PrivilegeDefinition def = new ImmutablePrivilegeDefinition(privilegeName, false, null);
            definitions.put(privilegeName, def);
        });
        AGGREGATE_PRIVILEGES.forEach((privilegeName, aggregatedNames) -> {
            PrivilegeDefinition def = new ImmutablePrivilegeDefinition(privilegeName, false, asList(aggregatedNames));
            definitions.put(privilegeName, def);
        });
        PrivilegeDefinition all = new ImmutablePrivilegeDefinition(JCR_ALL, false, definitions.keySet());
        definitions.put(JCR_ALL, all);
        return definitions.values();
    }
}
