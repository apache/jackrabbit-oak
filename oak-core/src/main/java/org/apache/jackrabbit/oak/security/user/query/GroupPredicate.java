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
package org.apache.jackrabbit.oak.security.user.query;

import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GroupPredicate... TODO
 */
class GroupPredicate implements Predicate<Authorizable> {

    static final Logger log = LoggerFactory.getLogger(GroupPredicate.class);

    private final Group group;
    private final boolean declaredMembersOnly;

    GroupPredicate(UserManager userManager, String groupId, boolean declaredMembersOnly) throws RepositoryException {
        Authorizable authorizable = userManager.getAuthorizable(groupId);
        group = (authorizable == null || !authorizable.isGroup()) ? null : (Group) authorizable;
        this.declaredMembersOnly = declaredMembersOnly;
    }

    @Override
    public boolean apply(@Nullable Authorizable authorizable) {
        if (group != null && authorizable != null) {
            try {
                return (declaredMembersOnly) ? group.isDeclaredMember(authorizable) : group.isMember(authorizable);
            } catch (RepositoryException e) {
                log.debug("Cannot determine group membership for {}", authorizable, e.getMessage());
            }
        }
        return false;
    }
}