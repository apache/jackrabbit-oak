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

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.iterator.RangeIteratorAdapter;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipProvider;
import org.jetbrains.annotations.NotNull;

import javax.jcr.RepositoryException;
import java.util.Collections;
import java.util.Iterator;

import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.REP_PRINCIPAL_NAME;

class EveryoneMembershipProvider implements DynamicMembershipProvider {

    private final UserManager userManager;
    private final String repPrincipalName;
    
    EveryoneMembershipProvider(@NotNull UserManager userManager, @NotNull NamePathMapper namePathMapper)  {
        this.userManager = userManager;
        this.repPrincipalName = namePathMapper.getJcrName(REP_PRINCIPAL_NAME);
    }
    
    @Override
    public boolean coversAllMembers(@NotNull Group group) {
        return Utils.isEveryone(group);
    }

    @Override
    public @NotNull Iterator<Authorizable> getMembers(@NotNull Group group, boolean includeInherited) throws RepositoryException {
        if (Utils.isEveryone(group)) {
            Iterator<Authorizable> result = Iterators.filter(userManager.findAuthorizables(repPrincipalName, null, UserManager.SEARCH_TYPE_AUTHORIZABLE), Predicates.notNull());
            return Iterators.filter(result, authorizable -> !Utils.isEveryone(authorizable));
        } else {
            return RangeIteratorAdapter.EMPTY;
        }
    }

    @Override
    public boolean isMember(@NotNull Group group, @NotNull Authorizable authorizable, boolean includeInherited) throws RepositoryException {
        return Utils.isEveryone(group);
    }

    @Override
    public @NotNull Iterator<Group> getMembership(@NotNull Authorizable authorizable, boolean includeInherited) throws RepositoryException {
        Authorizable everyoneGroup = userManager.getAuthorizable(EveryonePrincipal.getInstance());
        if (everyoneGroup instanceof Group) {
            return new RangeIteratorAdapter(Collections.singleton((Group) everyoneGroup));
        } else {
            return RangeIteratorAdapter.EMPTY;
        }
    }
}
