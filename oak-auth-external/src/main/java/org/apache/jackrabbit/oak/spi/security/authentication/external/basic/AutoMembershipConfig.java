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
package org.apache.jackrabbit.oak.spi.security.authentication.external.basic;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * Optional extension of the {@link org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig.Authorizable#getAutoMembership()} 
 * that allows to define conditional auto-membership based on the nature of a given {@link Authorizable}.
 */
public interface AutoMembershipConfig {

    /**
     * Name of the configuration property that defines the name of the synchronization handler for which this instance of 
     * {@link AutoMembershipConfig} takes effect.
     *
     * @see AutoMembershipAware
     */
    String PARAM_SYNC_HANDLER_NAME = "sync.handlerName";

    /**
     * Returns the name of the sync handler for which this configuration takes effect.
     * 
     * @return the sync handler name.
     * @see #PARAM_SYNC_HANDLER_NAME
     */
    String getName();        

    /**
     * Returns the group ids which the given synchronized external user/group should automatically be added as member.
     * 
     * @param authorizable An external identity that has been synchronized into the repository.
     * @return A set of group ids which define the automatic membership for the given authorizable.
     */
    @NotNull
    Set<String> getAutoMembership(@NotNull Authorizable authorizable);

    /**
     * Best-effort attempt to retrieve all automatically defined members of the given {@link Group}.
     * 
     * @param userManager The user manager associated with the given {@code group}.
     * @param group The target group for which the known automatically added synced identities should be retrieved. 
     * @return An iterator of synced external identities. If the given group is not configured to hold any automatic
     * members or if the implementation is not able to determined the members an empty iterator is returned.
     * @see org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipProvider#getMembers(Group, boolean)
     */
    Iterator<Authorizable> getAutoMembers(@NotNull UserManager userManager, @NotNull Group group);
    
    AutoMembershipConfig EMPTY = new AutoMembershipConfig() {

        @Override
        public @NotNull String getName() {
            return "";
        }

        @Override
        public @NotNull Set<String> getAutoMembership(@NotNull Authorizable authorizable) {
            return Collections.emptySet();
        }

        @Override
        public Iterator<Authorizable> getAutoMembers(@NotNull UserManager userManager, @NotNull Group group) {
            return Collections.emptyIterator();
        }
    };
    
}