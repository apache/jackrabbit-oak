/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import java.security.Principal;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Objects;
import javax.jcr.RepositoryException;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is almost a copy of the CachedGroupPrincipal class from the oak-core module.
 */
final class CachedGroupPrincipal extends PrincipalImpl implements GroupPrincipal, ItemBasedPrincipal {

    private static final Logger log = LoggerFactory.getLogger(CachedGroupPrincipal.class);

    private Group group;
    private UserManager userManager;

    public CachedGroupPrincipal(@NotNull String principalName, @NotNull UserManager userManager) {
        super(principalName);
        this.userManager = userManager;
    }

    @Override
    public boolean isMember(@NotNull Principal principal) {
        boolean isMember = false;
        try {
            Authorizable a = userManager.getAuthorizable(principal);
            if (a != null) {
                Group g = getGroup();
                return g != null && g.isMember(a);
            }
        } catch (RepositoryException e) {
            log.warn("Failed to determine group membership: {}", e.getMessage());
        }

        // principal doesn't represent a known authorizable or an error occurred.
        return isMember;
    }

    @Override
    public @NotNull Enumeration<? extends Principal> members() {
        final Iterator<Authorizable> members;
        try {
            Group g = getGroup();
            members = (g == null) ? Collections.emptyIterator() : g.getMembers();
        } catch (RepositoryException e) {
            // should not occur.
            String msg = "Unable to retrieve Group members: " + e.getMessage();
            log.error(msg);
            throw new IllegalStateException(msg, e);
        }

        Iterator<Principal> principals = Iterators.transform(members, authorizable -> {
            if (authorizable == null) {
                return null;
            }
            try {
                return authorizable.getPrincipal();
            } catch (RepositoryException e) {
                String msg = "Internal error while retrieving principal: " + e.getMessage();
                log.error(msg);
                throw new IllegalStateException(msg, e);
            }
        });
        return Iterators.asEnumeration(Iterators.filter(principals, Objects::nonNull));
    }

    private Group getGroup() throws RepositoryException {
        if (group == null) {
            Authorizable authorizable = userManager.getAuthorizable(new PrincipalImpl(getName()));
            if (authorizable != null && authorizable.isGroup()) {
                group = (Group) authorizable;
            }
        }
        return group;
    }

    @Override
    public @NotNull String getPath() throws RepositoryException {
        return getGroup().getPath();
    }
}
