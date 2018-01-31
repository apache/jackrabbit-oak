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
package org.apache.jackrabbit.oak.security.authorization.permission;

import static com.google.common.collect.Lists.newArrayList;

import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;

import com.google.common.collect.ImmutableSet;

public class MountPermissionProvider extends PermissionProviderImpl {

    @Nonnull
    public static String getPermissionRootName(@Nonnull Mount mount, @Nonnull String workspace) {
        if (mount.isDefault()) {
            return workspace;
        } else {
            return mount.getPathFragmentName() + "-" + workspace;
        }
    }

    private final MountInfoProvider mountInfoProvider;

    public MountPermissionProvider(@Nonnull Root root, @Nonnull String workspaceName,
                                   @Nonnull Set<Principal> principals, @Nonnull RestrictionProvider restrictionProvider,
                                   @Nonnull ConfigurationParameters options, @Nonnull Context ctx,
                                   @Nonnull MountInfoProvider mountInfoProvider,
                                   @Nonnull RootProvider rootProvider) {
        super(root, workspaceName, principals, restrictionProvider, options, ctx, rootProvider);
        this.mountInfoProvider = mountInfoProvider;
    }

    @Override
    protected PermissionStore getPermissionStore(Root root, String workspaceName,
            RestrictionProvider restrictionProvider) {
        List<PermissionStoreImpl> stores = newArrayList();
        stores.add(new PermissionStoreImpl(root, workspaceName, restrictionProvider));
        for (Mount m : mountInfoProvider.getNonDefaultMounts()) {
            String psRoot = getPermissionRootName(m, workspaceName);
            PermissionStoreImpl ps = new PermissionStoreImpl(root, psRoot, restrictionProvider);
            stores.add(ps);
        }
        return new MountPermissionStore(stores);
    }

    private static class MountPermissionStore implements PermissionStore {

        private final List<PermissionStoreImpl> stores;

        public MountPermissionStore(List<PermissionStoreImpl> stores) {
            this.stores = stores;
        }

        @Override
        public Collection<PermissionEntry> load(Collection<PermissionEntry> entries, String principalName,
                String path) {
            for (PermissionStoreImpl store : stores) {
                Collection<PermissionEntry> col = store.load(null, principalName, path);
                if (col != null && !col.isEmpty()) {
                    return col;
                }
            }
            return ImmutableSet.of();
        }

        @Override
        public PrincipalPermissionEntries load(String principalName) {
            PrincipalPermissionEntries ppe = new PrincipalPermissionEntries();
            for (PermissionStoreImpl store : stores) {
                ppe.getEntries().putAll(store.load(principalName).getEntries());
            }
            ppe.setFullyLoaded(true);
            return ppe;
        }

        @Override
        public long getNumEntries(String principalName, long max) {
            long num = 0;
            for (PermissionStoreImpl store : stores) {
                num += store.getNumEntries(principalName, max);
                if (num >= max) {
                    break;
                }
            }
            return num;
        }

        @Override
        public void flush(Root root) {
            for (PermissionStoreImpl store : stores) {
                store.flush(root);
            }
        }
    }
}
