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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads and writes privilege definitions from and to the repository content
 * without applying any validation.
 */
public class PrivilegeDefinitionStore implements PrivilegeConstants {

    private static final Logger log = LoggerFactory.getLogger(PrivilegeDefinitionStore.class);

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
    private final Map<PrivilegeBits, Set<String>> bitsToNames = new HashMap<PrivilegeBits, Set<String>>();

    private PrivilegeBits next;

    public PrivilegeDefinitionStore(Root root) {
        this.root = root;
        Tree privilegesTree = getPrivilegesTree();
        if (privilegesTree != null && privilegesTree.hasProperty(REP_NEXT)) {
            next = PrivilegeBits.getInstance(privilegesTree);
        } else {
            next = PrivilegeBits.BUILT_IN.get(REP_USER_MANAGEMENT).nextBits();
        }
    }

    /**
     * Returns the root tree for all privilege definitions stored in the content
     * repository.
     *
     * @return The privileges root.
     */
    @CheckForNull
    Tree getPrivilegesTree() {
        return root.getTree(PRIVILEGES_PATH);
    }

    /**
     * @param privilegeNames
     * @return
     */
    @Nonnull
    public PrivilegeBits getBits(@Nonnull String... privilegeNames) {
        if (privilegeNames.length == 0) {
            return PrivilegeBits.EMPTY;
        }

        Tree privilegesTree = getPrivilegesTree();
        if (privilegesTree == null) {
            return PrivilegeBits.EMPTY;
        }
        PrivilegeBits bits = PrivilegeBits.getInstance();
        for (String privilegeName : privilegeNames) {
            Tree defTree = privilegesTree.getChild(privilegeName);
            if (defTree != null) {
                bits.add(PrivilegeBits.getInstance(defTree));
            }
        }
        return bits.unmodifiable();
    }

    /**
     * Resolve the given privilege bits to a set of privilege names.
     *
     * @param privilegeBits An instance of privilege bits.
     * @return The names of the registed privileges associated with the given
     *         bits. Any bits that don't have a corresponding privilege definition will
     *         be ignored.
     */
    @Nonnull
    public Set<String> getPrivilegeNames(PrivilegeBits privilegeBits) {
        if (privilegeBits == null || privilegeBits.isEmpty()) {
            return Collections.emptySet();
        } else if (bitsToNames.containsKey(privilegeBits)) {
            // matches all built-in aggregates and single built-in privileges
            return bitsToNames.get(privilegeBits);
        } else {
            Tree privilegesTree = getPrivilegesTree();
            if (privilegesTree == null) {
                return Collections.emptySet();
            }

            if (bitsToNames.isEmpty()) {
                for (Tree child : privilegesTree.getChildren()) {
                    bitsToNames.put(PrivilegeBits.getInstance(child), Collections.singleton(child.getName()));
                }
            }

            Set<String> privilegeNames;
            if (bitsToNames.containsKey(privilegeBits)) {
                privilegeNames = bitsToNames.get(privilegeBits);
            } else {
                privilegeNames = new HashSet<String>();
                Set<String> aggregates = new HashSet<String>();
                for (Tree child : privilegesTree.getChildren()) {
                    PrivilegeBits bits = PrivilegeBits.getInstance(child);
                    if (privilegeBits.includes(bits)) {
                        privilegeNames.add(child.getName());
                        if (child.hasProperty(REP_AGGREGATES)) {
                            aggregates.addAll(readDefinition(child).getDeclaredAggregateNames());
                        }
                    }
                }
                privilegeNames.removeAll(aggregates);
                bitsToNames.put(privilegeBits.unmodifiable(), ImmutableSet.copyOf(privilegeNames));
            }
            return privilegeNames;
        }
    }

    /**
     * Read all registered privilege definitions from the content.
     *
     * @return All privilege definitions stored in the content.
     */
    @Nonnull
    public Map<String, PrivilegeDefinition> readDefinitions() {
        Tree privilegesTree = getPrivilegesTree();
        if (privilegesTree == null) {
            return Collections.emptyMap();
        } else {
            Map<String, PrivilegeDefinition> definitions = new HashMap<String, PrivilegeDefinition>();
            for (Tree child : privilegesTree.getChildren()) {
                PrivilegeDefinition def = readDefinition(child);
                definitions.put(def.getName(), def);
            }
            return definitions;
        }
    }

    /**
     * Retrieve the privilege definition with the specified {@code privilegeName}.
     *
     * @param privilegeName The name of a registered privilege definition.
     * @return The privilege definition with the specified name or {@code null}
     *         if the name doesn't refer to a registered privilege.
     */
    @CheckForNull
    public PrivilegeDefinition readDefinition(String privilegeName) {
        Tree privilegesTree = getPrivilegesTree();
        if (privilegesTree == null) {
            return null;
        } else {
            Tree definitionTree = privilegesTree.getChild(privilegeName);
            return (definitionTree == null) ? null : readDefinition(definitionTree);
        }
    }

    /**
     * @param definitionTree
     * @return
     */
    @Nonnull
    static PrivilegeDefinition readDefinition(@Nonnull Tree definitionTree) {
        String name = definitionTree.getName();
        boolean isAbstract = TreeUtil.getBoolean(definitionTree, REP_IS_ABSTRACT);
        String[] declAggrNames = TreeUtil.getStrings(definitionTree, REP_AGGREGATES);

        return new PrivilegeDefinitionImpl(name, isAbstract, declAggrNames);
    }

    /**
     * Write the given privilege definition to the repository content.
     *
     * @param definition The new privilege definition.
     * @throws RepositoryException If the definition can't be written.
     */
    public void writeDefinition(PrivilegeDefinition definition) throws RepositoryException {
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

    //------------------------------------------------------------< private >---

    /**
     * @param definitions
     * @throws RepositoryException
     */
    private void writeDefinitions(Iterable<PrivilegeDefinition> definitions) throws RepositoryException {
        try {
            // make sure the privileges path is defined
            Tree privilegesTree = getPrivilegesTree();
            if (privilegesTree == null) {
                throw new RepositoryException("Privilege store does not exist.");
            }
            NodeUtil privilegesNode = new NodeUtil(getPrivilegesTree());
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
            next.writeTo(privilegesTree);

            // delegate validation to the commit validation (see above)
            root.commit();

        } catch (CommitFailedException e) {
            Throwable t = e.getCause();
            if (t instanceof RepositoryException) {
                throw (RepositoryException) t;
            } else {
                throw new RepositoryException(e);
            }
        }
    }

    private void writePrivilegeNode(NodeUtil privilegesNode, PrivilegeDefinition definition) {
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
            bits = getBits(declAggrNames);
        } else {
            bits = next;
            next = bits.nextBits();
        }
        bits.writeTo(privNode.getTree());
    }

    private static Collection<PrivilegeDefinition> getBuiltInDefinitions() {
        Map<String, PrivilegeDefinition> definitions = new LinkedHashMap<String, PrivilegeDefinition>();
        for (String privilegeName : NON_AGGR_PRIVILEGES) {
            PrivilegeDefinition def = new PrivilegeDefinitionImpl(privilegeName, false);
            definitions.put(privilegeName, def);
        }
        for (String privilegeName : AGGREGATE_PRIVILEGES.keySet()) {
            PrivilegeDefinition def = new PrivilegeDefinitionImpl(privilegeName, false, AGGREGATE_PRIVILEGES.get(privilegeName));
            definitions.put(privilegeName, def);
        }
        PrivilegeDefinition all = new PrivilegeDefinitionImpl(JCR_ALL, false, definitions.keySet());
        definitions.put(JCR_ALL, all);
        return definitions.values();
    }
}
