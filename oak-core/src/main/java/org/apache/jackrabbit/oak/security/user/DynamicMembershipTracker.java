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

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipProvider;
import org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipService;
import org.apache.jackrabbit.oak.spi.whiteboard.AbstractServiceTracker;
import org.jetbrains.annotations.NotNull;

import javax.jcr.RepositoryException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DynamicMembershipTracker extends AbstractServiceTracker<DynamicMembershipService>  implements DynamicMembershipService {

    public DynamicMembershipTracker() {
        super(DynamicMembershipService.class);
    }

    @Override
    @NotNull
    public DynamicMembershipProvider getDynamicMembershipProvider(@NotNull Root root, @NotNull UserManager userManager, @NotNull NamePathMapper namePathMapper) {
        DynamicMembershipProvider defaultProvider = new EveryoneMembershipProvider(userManager, namePathMapper);
        List<DynamicMembershipService> services = getServices();
        if (services.isEmpty()) {
            return defaultProvider;
        } else {
            return createProvider(root, userManager, namePathMapper, defaultProvider, services);
        }
    }
    
    /**
     * Initialize DynamicMembershipProvider instances.
     * NOTE: Since providers are are created on demand for a given {@code UserManager} instance and Session instances are 
     * expected to be short-lived compared to the frequency of service registrations, no effort is made to keep the 
     * the list updated.
     */
    private static DynamicMembershipProvider createProvider(@NotNull Root root, @NotNull UserManager userManager, 
                                                            @NotNull NamePathMapper namePathMapper, 
                                                            @NotNull DynamicMembershipProvider defaultProvider, 
                                                            @NotNull List<DynamicMembershipService> services) {
        List<DynamicMembershipProvider> providers = new ArrayList<>(1+services.size());
        providers.add(defaultProvider);
        for (DynamicMembershipService service : services) {
            DynamicMembershipProvider dmp = service.getDynamicMembershipProvider(root, userManager, namePathMapper);
            if (DynamicMembershipProvider.EMPTY != dmp) {
                providers.add(dmp);
            }
        }

        if (providers.size() == 1) {
            return defaultProvider;
        } else {
            return new CompositeProvider(providers);
        }
    }

    private static class CompositeProvider implements DynamicMembershipProvider {
        
        private final List<DynamicMembershipProvider> providers;
        
        private CompositeProvider(@NotNull List<DynamicMembershipProvider> providers) {
            this.providers = providers;
        }
        
        @Override
        public boolean coversAllMembers(@NotNull Group group) {
            for (DynamicMembershipProvider provider : providers) {
                if (provider.coversAllMembers(group)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public @NotNull Iterator<Authorizable> getMembers(@NotNull Group group, boolean includeInherited) throws RepositoryException {
            int size = providers.size();
            Iterator<Authorizable>[] members = new Iterator[size];
            for (int i = 0; i < size; i++) {
                members[i] = providers.get(i).getMembers(group, includeInherited);
            }
            return Iterators.concat(members);
        }

        @Override
        public boolean isMember(@NotNull Group group, @NotNull Authorizable authorizable, boolean includeInherited) throws RepositoryException {
            for (DynamicMembershipProvider provider : providers) {
                if (provider.isMember(group, authorizable, includeInherited)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public @NotNull Iterator<Group> getMembership(@NotNull Authorizable authorizable, boolean includeInherited) throws RepositoryException {
            int size = providers.size();
            Iterator<Group>[] groups = new Iterator[size];
            for (int i = 0; i < size; i++) {
                groups[i] = providers.get(i).getMembership(authorizable, includeInherited);
            }
            return Iterators.concat(groups);
        }
    }
}
