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
package org.apache.jackrabbit.oak.jcr.security.privileges;

import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.jcr.NodeImpl;
import org.apache.jackrabbit.oak.jcr.SessionDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.Workspace;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * PrivilegeManagerImpl...
 */
public class PrivilegeManagerImpl implements PrivilegeManager {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(PrivilegeManagerImpl.class);

    // TODO: move to an appropriate place
    // TODO: proper namespace handling once this is present in oak-jcr
    private static final String PRIVILEGE_ROOT = Workspace.NAME_SYSTEM_NODE + "/{internal}privileges";

    private static final String NT_REP_PRIVILEGE = "rep:Privilege";
    private static final String REP_IS_ABSTRACT = "rep:isAbstract";
    private static final String REP_CONTAINS = "rep:contains";

    private final SessionDelegate sessionDelegate;

    public PrivilegeManagerImpl(SessionDelegate sessionDelegate) {
        this.sessionDelegate = sessionDelegate;
        // TODO: add additional validation ??
    }

    @Override
    public Privilege[] getRegisteredPrivileges() throws RepositoryException {
        Map<String, Privilege> privileges = new HashMap<String, Privilege>();
        NodeIterator it = getPrivilegeRoot().getNodes();
        while (it.hasNext()) {
            Node privilegeNode = it.nextNode();
            Privilege p = getPrivilege(privilegeNode, privileges);
            if (p != null) {
                privileges.put(p.getName(),  p);
            }
        }
        return privileges.values().toArray(new Privilege[privileges.size()]);
    }

    @Override
    public Privilege getPrivilege(String privilegeName) throws RepositoryException {
        NodeImpl privilegeRoot = getPrivilegeRoot();
        if (privilegeRoot.hasNode(privilegeName)) {
            return getPrivilege(privilegeRoot.getNode(privilegeName), new HashMap<String, Privilege>(1));
        } else {
            throw new AccessControlException("No such privilege " + privilegeName);
        }
    }

    @Override
    public Privilege registerPrivilege(String privilegeName, boolean isAbstract,
                                       String[] declaredAggregateNames)
            throws RepositoryException {

        // TODO
        return null;
    }

    //--------------------------------------------------------------------------
    private NodeImpl getPrivilegeRoot() throws RepositoryException {
        return (NodeImpl) sessionDelegate.getSession().getNode(PRIVILEGE_ROOT);
    }

    private Privilege getPrivilege(Node privilegeNode, Map<String, Privilege> collected) throws RepositoryException {
        Privilege privilege = null;
        if (privilegeNode.isNodeType(NT_REP_PRIVILEGE)) {
            String name = privilegeNode.getName();
            boolean isAbstract = privilegeNode.getProperty(REP_IS_ABSTRACT).getBoolean();

            Set<String> declaredAggrNames;
            if (privilegeNode.hasProperty(REP_CONTAINS)) {
                Value[] vs = privilegeNode.getProperty(REP_CONTAINS).getValues();
                declaredAggrNames = new HashSet<String>(vs.length);
                for (Value v : vs) {
                    String privName = v.getString();
                    if (getPrivilegeRoot().hasNode(privName)) {
                        declaredAggrNames.add(privName);
                    }
                }
            } else {
                declaredAggrNames = Collections.emptySet();
            }
            privilege = new PrivilegeImpl(name, isAbstract, declaredAggrNames);
        }

        return privilege;
    }

    //--------------------------------------------------------------------------
    private class PrivilegeImpl implements Privilege {

        private final String name;
        private final boolean isAbstract;
        private final Set<String> declaredAggregateNames;

        private Privilege[] declaredAggregates;

        private PrivilegeImpl(String name, boolean isAbstract, Set<String> declaredAggregateNames) {
            this.name = name;
            this.isAbstract = isAbstract;
            this.declaredAggregateNames = declaredAggregateNames;
        }

        //------------------------------------------------------< Privilege >---
        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean isAbstract() {
            return isAbstract;
        }

        @Override
        public boolean isAggregate() {
            return !declaredAggregateNames.isEmpty();
        }

        @Override
        public Privilege[] getDeclaredAggregatePrivileges() {
            if (declaredAggregates == null) {
                Set<Privilege> dagrPrivs = new HashSet<Privilege>(declaredAggregateNames.size());
                for (String pName : declaredAggregateNames) {
                    try {
                        dagrPrivs.add(getPrivilege(pName));
                    } catch (RepositoryException e) {
                        log.warn("Error while retrieving privilege "+ pName +" contained in " + getName(), e.getMessage());
                    }
                }
                declaredAggregates = dagrPrivs.toArray(new Privilege[dagrPrivs.size()]);
            }
            return declaredAggregates;
        }

        @Override
        public Privilege[] getAggregatePrivileges() {
            Set<Privilege> aggr = new HashSet<Privilege>();
            for (Privilege decl : getDeclaredAggregatePrivileges()) {
                aggr.add(decl);
                if (decl.isAggregate()) {
                    // TODO: defensive check to prevent circular aggregation
                    aggr.addAll(Arrays.asList(decl.getAggregatePrivileges()));
                }
            }
            return aggr.toArray(new Privilege[aggr.size()]);
        }
    }
}