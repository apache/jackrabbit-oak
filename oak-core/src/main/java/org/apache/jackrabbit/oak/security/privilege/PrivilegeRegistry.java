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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.Nonnull;
import javax.jcr.NamespaceRegistry;
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
import org.apache.jackrabbit.oak.plugins.name.NamespaceRegistryImpl;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.oak.util.TODO;
import org.apache.jackrabbit.util.Text;

/**
 * PrivilegeProviderImpl... TODO
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
        this.reader = new PrivilegeDefinitionReader(contentSession);

        // TODO: define if/how built-in privileges are reflected in the mk
        // TODO: define where custom privileges are being stored.
        // TODO: define if custom privileges are read with editing content session (thus enforcing read permissions)

        for (String privilegeName : SIMPLE_PRIVILEGES) {
            PrivilegeDefinition def = new PrivilegeDefinitionImpl(privilegeName, false);
            definitions.put(privilegeName, def);
        }

        for (String privilegeName : AGGREGATE_PRIVILEGES.keySet()) {
            PrivilegeDefinition def = new PrivilegeDefinitionImpl(privilegeName, false, AGGREGATE_PRIVILEGES.get(privilegeName));
            definitions.put(privilegeName, def);
        }

        CoreValueFactory vf = contentSession.getCoreValueFactory();
        Root root = contentSession.getCurrentRoot();

        Tree privilegesTree = root.getTree(PRIVILEGES_PATH);
        if (privilegesTree == null) {
            // backwards compatibility: read privileges from file system and update
            // the content tree.
            try {
                NodeUtil system = new NodeUtil(root.getTree(JcrConstants.JCR_SYSTEM), contentSession);
                NodeUtil privNode = system.addChild(REP_PRIVILEGES, NT_REP_PRIVILEGES);

                migrateCustomPrivileges(privNode);

                root.commit(DefaultConflictHandler.OURS);
            } catch (IOException e) {
                throw new RepositoryException(e);
            } catch (CommitFailedException e) {
                throw new RepositoryException(e);
            }
        }

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
        // TODO: add proper implementation including
        // - permission check (possibly delegate to a commit validator),
        // - validate definition (possibly delegate to commit validator)
        // - persist the custom definition
        // - recalculate jcr:all privilege
        return TODO.dummyImplementation().call(new Callable<PrivilegeDefinition>() {
            @Override
            public PrivilegeDefinition call() throws Exception {
                PrivilegeDefinition definition = new PrivilegeDefinitionImpl(
                        privilegeName, isAbstract,
                        new HashSet<String>(declaredAggregateNames));
                definitions.put(privilegeName, definition);
                return definition;
            }
        });
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

        // additional validation of the definition include:
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

    private void migrateCustomPrivileges(NodeUtil privilegesNode) throws RepositoryException, IOException, CommitFailedException {
        InputStream stream = null;
        // TODO: user proper path to jr2 custom privileges stored in fs
        // jr2 used to be:
        // new FileSystemResource(fs, "/privileges/custom_privileges.xml").getInputStream()
        if (stream != null) {
            try {
                NamespaceRegistry  nsRegistry = new NamespaceRegistryImpl(contentSession);
                Map<String, PrivilegeDefinition> custom = PrivilegeDefinitionReader.readCustomDefinitons(stream, nsRegistry);
                writeDefinitions(privilegesNode, custom);
            } finally {
                stream.close();
            }
        }
    }

    private void updateJcrAllPrivilege() {
        // TODO: add proper implementation taking custom privileges into account.
        definitions.put(JCR_ALL, new PrivilegeDefinitionImpl(JCR_ALL, false,
                JCR_READ, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL,
                JCR_VERSION_MANAGEMENT, JCR_LOCK_MANAGEMENT, JCR_LIFECYCLE_MANAGEMENT,
                JCR_RETENTION_MANAGEMENT, JCR_WORKSPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT,
                JCR_NAMESPACE_MANAGEMENT, REP_PRIVILEGE_MANAGEMENT, REP_WRITE));
    }

    private void writeDefinitions(NodeUtil privilegesNode, Map<String, PrivilegeDefinition> definitions) {
        for (PrivilegeDefinition def : definitions.values()) {
            NodeUtil privNode = privilegesNode.addChild(def.getName(), NT_REP_PRIVILEGE);
            if (def.isAbstract()) {
                privNode.setBoolean(REP_IS_ABSTRACT, true);
            }
            String[] declAggrNames = def.getDeclaredAggregateNames();
            if (declAggrNames.length > 0) {
                privNode.setNames(REP_AGGREGATES, def.getDeclaredAggregateNames());
            }
        }
    }

    /**
     *
     * @param definition
     */
    private void validateDefinition(PrivilegeDefinition definition) {
        // TODO
        // - aggregate names refer to existing privileges
        // - aggregate names do not create cyclic dependencies
        // - aggregate names are not covered by an existing privilege definition
        // -
    }
}