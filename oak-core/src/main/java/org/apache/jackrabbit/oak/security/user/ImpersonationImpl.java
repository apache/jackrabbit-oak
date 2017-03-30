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
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.security.auth.Subject;

import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalIteratorAdapter;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.api.Type.STRINGS;

/**
 * ImpersonationImpl...
 */
class ImpersonationImpl implements Impersonation, UserConstants {

    private static final Logger log = LoggerFactory.getLogger(ImpersonationImpl.class);

    private final UserImpl user;
    private final PrincipalManager principalManager;

    ImpersonationImpl(@Nonnull UserImpl user) throws RepositoryException {
        this.user = user;
        this.principalManager = user.getUserManager().getPrincipalManager();
    }

    //------------------------------------------------------< Impersonation >---

    /**
     * @see org.apache.jackrabbit.api.security.user.Impersonation#getImpersonators()
     */
    @Nonnull
    @Override
    public PrincipalIterator getImpersonators() throws RepositoryException {
        Set<String> impersonators = getImpersonatorNames();
        if (impersonators.isEmpty()) {
            return PrincipalIteratorAdapter.EMPTY;
        } else {
            Set<Principal> s = new HashSet<Principal>();
            for (final String pName : impersonators) {
                Principal p = principalManager.getPrincipal(pName);
                if (p == null) {
                    log.debug("Impersonator " + pName + " does not correspond to a known Principal.");
                    p = new PrincipalImpl(pName);
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
    public boolean grantImpersonation(@Nonnull Principal principal) throws RepositoryException {
        if (!isValidPrincipal(principal)) {
            return false;
        }
        String principalName = principal.getName();
        // make sure user does not impersonate himself
        Tree userTree = user.getTree();
        PropertyState prop = userTree.getProperty(REP_PRINCIPAL_NAME);
        if (prop != null && prop.getValue(Type.STRING).equals(principalName)) {
            log.warn("Cannot grant impersonation to oneself.");
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
    public boolean revokeImpersonation(@Nonnull Principal principal) throws RepositoryException {
        String pName = principal.getName();

        Tree userTree = user.getTree();
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
    public boolean allows(@CheckForNull Subject subject) throws RepositoryException {
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
    @Nonnull
    private Set<String> getImpersonatorNames() {
        return getImpersonatorNames(user.getTree());
    }

    @Nonnull
    private Set<String> getImpersonatorNames(@Nonnull Tree userTree) {
        Set<String> princNames = new HashSet<String>();
        PropertyState impersonators = userTree.getProperty(REP_IMPERSONATORS);
        if (impersonators != null) {
            for (String v : impersonators.getValue(STRINGS)) {
                princNames.add(v);
            }
        }
        return princNames;
    }

    private void updateImpersonatorNames(@Nonnull Tree userTree, @Nonnull Set<String> principalNames) {
        if (principalNames == null || principalNames.isEmpty()) {
            userTree.removeProperty(REP_IMPERSONATORS);
        } else {
            userTree.setProperty(REP_IMPERSONATORS, principalNames, Type.STRINGS);
        }
    }

    private boolean isAdmin(@Nonnull Principal principal) {
        if (principal instanceof AdminPrincipal) {
            return true;
        } else if (principal instanceof Group) {
            return false;
        } else {
            try {
                Authorizable authorizable = user.getUserManager().getAuthorizable(principal);
                return authorizable != null && !authorizable.isGroup() && ((User) authorizable).isAdmin();
            } catch (RepositoryException e) {
                log.debug(e.getMessage());
                return false;
            }
        }
    }

    private boolean isValidPrincipal(@Nonnull Principal principal) {
        Principal p = null;
        // shortcut for TreeBasedPrincipal
        if (principal instanceof TreeBasedPrincipal) {
            try {
                Authorizable otherUser = user.getUserManager().getAuthorizable(principal);
                if (otherUser != null) {
                    p = otherUser.getPrincipal();
                }

            } catch (RepositoryException e) {
                log.debug(e.getMessage());
            }
        } else {
            p = principalManager.getPrincipal(principal.getName());
        }
        if (p == null) {
            log.debug("Cannot grant impersonation to an unknown principal.");
            return false;
        }
        if (p instanceof Group) {
            log.debug("Cannot grant impersonation to a principal that is a Group.");
            return false;
        }
        // make sure the given principal doesn't refer to the admin user.
        if (isAdmin(p)) {
            log.debug("Admin principal is already granted impersonation.");
            return false;
        }
        return true;
    }
}
