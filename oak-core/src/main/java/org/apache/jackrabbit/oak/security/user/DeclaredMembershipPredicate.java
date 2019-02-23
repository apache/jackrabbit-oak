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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.jcr.RepositoryException;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Predicate used to filter authorizables based on their declared group membership.
 */
public class DeclaredMembershipPredicate implements Predicate<Authorizable> {

    private static final Logger log = LoggerFactory.getLogger(DeclaredMembershipPredicate.class);

    private final MembershipProvider membershipProvider;
    private final Iterator<String> contentIdIterator;
    private final Set<String> declaredMemberContentIds = new HashSet<>();

    public DeclaredMembershipPredicate(UserManagerImpl userManager, String groupId) {
        this.membershipProvider = userManager.getMembershipProvider();
        Tree groupTree = membershipProvider.getByID(groupId, AuthorizableType.GROUP);
        if (groupTree == null) {
            contentIdIterator = Collections.emptyIterator();
        } else {
            contentIdIterator = membershipProvider.getDeclaredMemberContentIDs(membershipProvider.getByID(groupId, AuthorizableType.GROUP));
        }
    }

    @Override
    public boolean apply(@Nullable Authorizable authorizable) {
        String id = saveGetContentId(authorizable);
        if (id != null) {
            if (declaredMemberContentIds.contains(id)) {
                return true;
            } else {
                // not contained in ids that have already been processed => look
                // for occurrence in the remaining iterator entries.
                while (contentIdIterator.hasNext()) {
                    String memberContentId = contentIdIterator.next();
                    if (memberContentId != null) {
                        declaredMemberContentIds.add(memberContentId);
                        if (memberContentId.equals(id)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    @Nullable
    private String saveGetContentId(@Nullable Authorizable authorizable) {
        if (authorizable != null) {
            try {
                return membershipProvider.getContentID(authorizable.getID());
            } catch (RepositoryException e) {
                log.debug("Error while retrieving ID for authorizable {}", authorizable, e);
            }
        }
        return null;
    }
}
