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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.commons.iterator.AbstractLazyIterator;
import org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipProvider;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Wrapper around a members iterator that will resolve dynamic members of the contained groups.
 */
class InheritedMembersIterator extends AbstractLazyIterator<Authorizable> {

    private static final Logger log = LoggerFactory.getLogger(InheritedMembersIterator.class);
    
    private final Iterator<Authorizable> members;
    private final DynamicMembershipProvider dynamicMembershipProvider;
    private final List<Group> groups = new ArrayList<>();
    
    private Iterator<Authorizable> dynamicMembers;

    InheritedMembersIterator(@NotNull Iterator<Authorizable> members, @NotNull DynamicMembershipProvider dynamicMembershipProvider) {
        this.members = members;
        this.dynamicMembershipProvider = dynamicMembershipProvider;
    }

    @Override
    protected Authorizable getNext() {
        if (members.hasNext()) {
            Authorizable member = members.next();
            if (member.isGroup()) {
                rememberGroup((Group) member);
            }
            return member;
        }

        if (dynamicMembers == null || !dynamicMembers.hasNext()) {
            dynamicMembers = getNextDynamicMembersIterator();
        }

        if (dynamicMembers.hasNext()) {
            return dynamicMembers.next();
        }

        // all dynamic members have been processed
        return null;
    }
    
    private void rememberGroup(@NotNull Group group) {
        groups.add(group);
    }

    @NotNull
    private Iterator<Authorizable> getNextDynamicMembersIterator() {
        while (!groups.isEmpty()) {
            Group group = groups.remove(0);
            try {
                Iterator<Authorizable> it = dynamicMembershipProvider.getMembers(group, false);
                if (it.hasNext()) {
                    return it;
                }
            } catch (RepositoryException e) {
                log.error("Failed to retrieve dynamic members of group '{}'", Utils.getIdOrNull(group), e);
            }
        }
        return Collections.emptyIterator();
    }
}