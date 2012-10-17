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
package org.apache.jackrabbit.oak.security.user;

import java.security.Principal;
import java.security.acl.Group;
import java.util.HashSet;
import java.util.Set;
import javax.jcr.RepositoryException;
import javax.security.auth.Subject;

import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalIteratorAdapter;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.api.Type.STRINGS;

/**
 * ImpersonationImpl...
 */
class ImpersonationImpl implements Impersonation, UserConstants {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(ImpersonationImpl.class);

    private final String userId;
    private final UserProvider userProvider;
    private final PrincipalProvider principalProvider;

    ImpersonationImpl(String userId, UserProvider userProvider, PrincipalProvider principalProvider) {
        this.userId = userId;
        this.userProvider = userProvider;
        this.principalProvider = principalProvider;
    }

    //------------------------------------------------------< Impersonation >---
    /**
     * @see org.apache.jackrabbit.api.security.user.Impersonation#getImpersonators()
     */
    @Override
    public PrincipalIterator getImpersonators() throws RepositoryException {
        Set<String> impersonators = getImpersonatorNames();
        if (impersonators.isEmpty()) {
            return PrincipalIteratorAdapter.EMPTY;
        } else {
            Set<Principal> s = new HashSet<Principal>();
            for (final String pName : impersonators) {
                Principal p = principalProvider.getPrincipal(pName);
                if (p == null) {
                    log.debug("Impersonator " + pName + " does not correspond to a known Principal.");
                    p = new Principal() {
                        @Override
                        public String getName() {
                            return pName;
                        }
                    };
                }
                s.add(p);

            }
            return new PrincipalIteratorAdapter(s);
        }
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Impersonation#grantImpersonation(Principal)
     */
    @Override
    public boolean grantImpersonation(Principal principal) throws RepositoryException {
        String principalName = principal.getName();
        Principal p = principalProvider.getPrincipal(principalName);
        if (p == null) {
            log.debug("Cannot grant impersonation to an unknown principal.");
            return false;
        }
        if (p instanceof Group) {
            log.debug("Cannot grant impersonation to a principal that is a Group.");
            return false;
        }

        // make sure user does not impersonate himself
        Tree userTree = getUserTree();
        PropertyState prop = userTree.getProperty(REP_PRINCIPAL_NAME);
        if (prop != null && prop.getValue(Type.STRING).equals(principalName)) {
            log.warn("Cannot grant impersonation to oneself.");
            return false;
        }

        // make sure the given principal doesn't refer to the admin user.
        if (isAdmin(p)) {
            log.debug("Admin principal is already granted impersonation.");
            return false;
        }

        Set<String> impersonators = getImpersonatorNames(userTree);
        if (impersonators.add(principalName)) {
            updateImpersonatorNames(userTree, impersonators);
            return true;
        } else {
            return false;
        }
    }

    /**
     * @see Impersonation#revokeImpersonation(java.security.Principal)
     */
    @Override
    public boolean revokeImpersonation(Principal principal) throws RepositoryException {
        String pName = principal.getName();

        Tree userTree = getUserTree();
        Set<String> impersonators = getImpersonatorNames(userTree);
        if (impersonators.remove(pName)) {
            updateImpersonatorNames(userTree, impersonators);
            return true;
        } else {
            return false;
        }
    }

    /**
     * @see Impersonation#allows(javax.security.auth.Subject)
     */
    @Override
    public boolean allows(Subject subject) throws RepositoryException {
        if (subject == null) {
            return false;
        }

        Set<String> principalNames = new HashSet<String>();
        for (Principal principal : subject.getPrincipals()) {
            principalNames.add(principal.getName());
        }

        boolean allows = getImpersonatorNames().removeAll(principalNames);
        if (!allows) {
            // check if subject belongs to administrator user
            for (Principal principal : subject.getPrincipals()) {
                if (isAdmin(principal)) {
                    allows = true;
                    break;
                }
            }
        }
        return allows;
    }

    //------------------------------------------------------------< private >---
    private Set<String> getImpersonatorNames() throws RepositoryException {
        return getImpersonatorNames(getUserTree());
    }

    private Set<String> getImpersonatorNames(Tree userTree) {
        Set<String> princNames = new HashSet<String>();
        PropertyState impersonators = userTree.getProperty(REP_IMPERSONATORS);
        if (impersonators != null) {
            for (String v : impersonators.getValue(STRINGS)) {
                princNames.add(v);
            }
        }
        return princNames;
    }

    private void updateImpersonatorNames(Tree userTree, Set<String> principalNames) {
        if (principalNames == null || principalNames.isEmpty()) {
            userTree.removeProperty(REP_IMPERSONATORS);
        } else {
            userTree.setProperty(REP_IMPERSONATORS, principalNames, Type.STRINGS);
        }
    }

    private Tree getUserTree() throws RepositoryException {
        Tree userTree = userProvider.getAuthorizable(userId, AuthorizableType.USER);
        if (userTree == null) {
            throw new RepositoryException("UserId " + userId + " cannot be resolved to user.");
        }
        return userTree;
    }

    private boolean isAdmin(Principal principal) {
        if (principal == AdminPrincipal.INSTANCE) {
            return true;
        } else if (principal instanceof Group) {
            return false;
        } else {
            Tree authorizableTree = userProvider.getAuthorizableByPrincipal(principal);
            return authorizableTree != null && userProvider.isAdminUser(authorizableTree);
        }
    }
}
