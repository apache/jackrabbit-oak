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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.jcr.InvalidItemStateException;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import com.google.common.base.Strings;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.security.privilege.ImmutablePrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code PrivilegeManager} implementation reading from and storing privileges
 * into the repository.
 */
class PrivilegeManagerImpl implements PrivilegeManager {

    private static final Logger log = LoggerFactory.getLogger(PrivilegeManagerImpl.class);

    private final Root root;
    private final NamePathMapper namePathMapper;

    PrivilegeManagerImpl(Root root, NamePathMapper namePathMapper) {
        this.root = root;
        this.namePathMapper = namePathMapper;
    }

    //---------------------------------------------------< PrivilegeManager >---
    @NotNull
    @Override
    public Privilege[] getRegisteredPrivileges() {
        Set<Privilege> privileges = new HashSet<>();
        for (PrivilegeDefinition def : getPrivilegeDefinitions()) {
            privileges.add(getPrivilege(def));
        }
        return privileges.toArray(new Privilege[0]);
    }

    @NotNull
    @Override
    public Privilege getPrivilege(@NotNull String privilegeName) throws RepositoryException {
        PrivilegeDefinition def = getPrivilegeDefinition(PrivilegeUtil.getOakName(privilegeName, namePathMapper));
        if (def == null) {
            throw new AccessControlException("No such privilege " + privilegeName);
        } else {
            return getPrivilege(def);
        }
    }

    @NotNull
    @Override
    public Privilege registerPrivilege(@NotNull String privilegeName, boolean isAbstract,
                                       @Nullable String[] declaredAggregateNames) throws RepositoryException {
        if (root.hasPendingChanges()) {
            throw new InvalidItemStateException("Attempt to register a new privilege while there are pending changes.");
        }
        if (Strings.isNullOrEmpty(privilegeName)) {
            throw new RepositoryException("Invalid privilege name " + privilegeName);
        }
        PrivilegeDefinition definition = new ImmutablePrivilegeDefinition(PrivilegeUtil.getOakName(privilegeName, namePathMapper), isAbstract, PrivilegeUtil.getOakNames(declaredAggregateNames, namePathMapper));
        PrivilegeDefinitionWriter writer = new PrivilegeDefinitionWriter(getWriteRoot());
        writer.writeDefinition(definition);

        // refresh the current root to make sure the definition is visible
        root.refresh();
        return getPrivilege(definition);
    }

    //------------------------------------------------------------< private >---
    @NotNull
    private Root getWriteRoot() {
        return root.getContentSession().getLatestRoot();
    }

    @NotNull
    private Privilege getPrivilege(@NotNull PrivilegeDefinition definition) {
        return new PrivilegeImpl(definition);
    }

    @NotNull
    private PrivilegeDefinition[] getPrivilegeDefinitions() {
        Map<String, PrivilegeDefinition> definitions = getReader().readDefinitions();
        return definitions.values().toArray(new PrivilegeDefinition[0]);
    }

    @Nullable
    private PrivilegeDefinition getPrivilegeDefinition(@NotNull String oakName) {
        return getReader().readDefinition(oakName);
    }

    @NotNull
    private PrivilegeDefinitionReader getReader() {
        return new PrivilegeDefinitionReader(root);
    }

    //--------------------------------------------------------------------------

    /**
     * Privilege implementation based on a {@link org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition}.
     */
    private final class PrivilegeImpl implements Privilege {

        private final PrivilegeDefinition definition;

        private PrivilegeImpl(@NotNull PrivilegeDefinition definition) {
            this.definition = definition;
        }

        //------------------------------------------------------< Privilege >---
        @Override
        public String getName() {
            return namePathMapper.getJcrName(definition.getName());
        }

        @Override
        public boolean isAbstract() {
            return definition.isAbstract();
        }

        @Override
        public boolean isAggregate() {
            return !definition.getDeclaredAggregateNames().isEmpty();
        }

        @Override
        public Privilege[] getDeclaredAggregatePrivileges() {
            Set<String> declaredAggregateNames = definition.getDeclaredAggregateNames();
            Set<Privilege> declaredAggregates = new HashSet<>(declaredAggregateNames.size());
            for (String oakName : declaredAggregateNames) {
                if (oakName.equals(definition.getName())) {
                    log.warn("Found cyclic privilege aggregation -> ignore declared aggregate {}", oakName);
                    continue;
                }
                PrivilegeDefinition def = getPrivilegeDefinition(oakName);
                if (def != null) {
                    declaredAggregates.add(getPrivilege(def));
                } else {
                    log.warn("Invalid privilege '{}' in declared aggregates of '{}'", oakName, getName());
                }
            }
            return declaredAggregates.toArray(new Privilege[0]);
        }

        @Override
        public Privilege[] getAggregatePrivileges() {
            Set<Privilege> aggr = new HashSet<>();
            for (Privilege decl : getDeclaredAggregatePrivileges()) {
                aggr.add(decl);
                if (decl.isAggregate()) {
                    aggr.addAll(Arrays.asList(decl.getAggregatePrivileges()));
                }
            }
            return aggr.toArray(new Privilege[0]);
        }

        //---------------------------------------------------------< Object >---
        @Override
        public int hashCode() {
            return definition.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o instanceof PrivilegeImpl) {
                return definition.equals(((PrivilegeImpl) o).definition);
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return definition.getName();
        }
    }
}
