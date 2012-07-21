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
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.util.TODO;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeProvider;

import com.google.common.collect.ImmutableSet;

/**
 * PrivilegeProviderImpl... TODO
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
    private final Map<String, PrivilegeDefinition> definitions = new HashMap<String, PrivilegeDefinition>();

    public PrivilegeRegistry(ContentSession contentSession) {
        this.contentSession = contentSession;

        // TODO: define if/how built-in privileges are reflected in the mk
        // TODO: define where custom privileges are being stored.

        for (String privilegeName : SIMPLE_PRIVILEGES) {
            PrivilegeDefinition def = new PrivilegeDefinitionImpl(privilegeName, false);
            definitions.put(privilegeName, def);
        }

        for (String privilegeName : AGGREGATE_PRIVILEGES.keySet()) {
            PrivilegeDefinition def = new PrivilegeDefinitionImpl(privilegeName, false, AGGREGATE_PRIVILEGES.get(privilegeName));
            definitions.put(privilegeName, def);
        }

        // TODO: jcr:all needs to be recalculated if custom privileges are registered
        definitions.put(JCR_ALL, new PrivilegeDefinitionImpl(JCR_ALL, false,
            JCR_READ, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL,
            JCR_VERSION_MANAGEMENT, JCR_LOCK_MANAGEMENT, JCR_LIFECYCLE_MANAGEMENT,
            JCR_RETENTION_MANAGEMENT, JCR_WORKSPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT,
            JCR_NAMESPACE_MANAGEMENT, REP_PRIVILEGE_MANAGEMENT, REP_WRITE));
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
        // TODO: check permission, validate and persist the custom definition
        return TODO.dummyImplementation().call(new Callable<PrivilegeDefinition>() {
            @Override
            public PrivilegeDefinition call() throws Exception {
                PrivilegeDefinition definition = new PrivilegeDefinitionImpl(
                        privilegeName, isAbstract,
                        new HashSet<String>(declaredAggregateNames));
                // TODO: update jcr:all
                definitions.put(privilegeName, definition);
                return definition;
            }
        });
    }


    //--------------------------------------------------------------------------

    private static class PrivilegeDefinitionImpl implements PrivilegeDefinition {

        private final String name;
        private final boolean isAbstract;
        private final Set<String> declaredAggregateNames;

        private PrivilegeDefinitionImpl(String name, boolean isAbstract,
                                        Set<String> declaredAggregateNames) {
            this.name = name;
            this.isAbstract = isAbstract;
            this.declaredAggregateNames = declaredAggregateNames;
        }

        private PrivilegeDefinitionImpl(String name, boolean isAbstract,
                                        String... declaredAggregateNames) {
            this(name, isAbstract, ImmutableSet.copyOf(declaredAggregateNames));
        }

        //--------------------------------------------< PrivilegeDefinition >---
        @Nonnull
        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean isAbstract() {
            return isAbstract;
        }

        @Nonnull
        @Override
        public String[] getDeclaredAggregateNames() {
            return declaredAggregateNames.toArray(new String[declaredAggregateNames.size()]);
        }

        //---------------------------------------------------------< Object >---
        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + (isAbstract ? 1 : 0);
            result = 31 * result + declaredAggregateNames.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o instanceof PrivilegeDefinitionImpl) {
                PrivilegeDefinitionImpl other = (PrivilegeDefinitionImpl) o;
                return name.equals(other.name) &&
                        isAbstract == other.isAbstract &&
                        declaredAggregateNames.equals(other.declaredAggregateNames);
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return "PrivilegeDefinition: " + name;
        }
    }
}