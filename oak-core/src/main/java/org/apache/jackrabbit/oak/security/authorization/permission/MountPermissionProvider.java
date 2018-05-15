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

import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.LongUtils;
import org.apache.jackrabbit.oak.security.authorization.ProviderCtx;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;

import static com.google.common.collect.Lists.newArrayList;

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
                                   @Nonnull ProviderCtx providerCtx) {
        super(root, workspaceName, principals, restrictionProvider, options, ctx, providerCtx);
        this.mountInfoProvider = providerCtx.getMountInfoProvider();
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

        @Nonnull
        @Override
        public Collection<PermissionEntry> load(@Nonnull String principalName,
                                                @Nonnull String path) {
            for (PermissionStoreImpl store : stores) {
                Collection<PermissionEntry> col = store.load(principalName, path);
                if (col != null && !col.isEmpty()) {
                    return col;
                }
            }
            return null;
        }

        @Nonnull
        @Override
        public PrincipalPermissionEntries load(@Nonnull String principalName) {
            PrincipalPermissionEntries ppe = new PrincipalPermissionEntries();
            for (PermissionStoreImpl store : stores) {
                ppe.putAllEntries(store.load(principalName).getEntries());
            }
            ppe.setFullyLoaded(true);
            return ppe;
        }

        @Nonnull
        @Override
        public NumEntries getNumEntries(@Nonnull String principalName, long max) {
            long num = 0;
            boolean isExact = true;
            for (PermissionStoreImpl store : stores) {
                NumEntries ne = store.getNumEntries(principalName, max);
                num = LongUtils.safeAdd(num, ne.size);
                if (!ne.isExact) {
                    isExact = false;
                }
                // if any of the stores doesn't reveal the exact number and max
                // is reached, stop asking the remaining stores.
                // as long as every store is reporting the exact number continue
                // in order to (possibly) be able to return the exact number.
                if (num >= max && !isExact) {
                    break;
                }
            }
            return NumEntries.valueOf(num, isExact);
        }

        @Override
        public void flush(@Nonnull Root root) {
            for (PermissionStoreImpl store : stores) {
                store.flush(root);
            }
        }
    }
}
