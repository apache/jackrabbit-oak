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
import java.security.acl.Group;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.util.Text;

import com.google.common.collect.Iterators;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * TODO: WIP
 */
class CompiledPermissionImpl implements CompiledPermissions, PermissionConstants {

    private final Set<Principal> principals;

    private final Map<String, Tree> userTrees;
    private final Map<String, Tree> groupTrees;

    private final String[] readPathsCheckList;

    private PrivilegeBitsProvider bitsProvider;

    private final PermissionStore userStore;
    private final PermissionStore groupStore;

    CompiledPermissionImpl(@Nonnull Set<Principal> principals,
                           @Nonnull ImmutableTree permissionsTree,
                           @Nonnull PrivilegeBitsProvider bitsProvider,
                           @Nonnull RestrictionProvider restrictionProvider,
                           @Nonnull Set<String> readPaths) {
        checkArgument(!principals.isEmpty());
        this.principals = principals;
        this.bitsProvider = bitsProvider;
        readPathsCheckList = new String[readPaths.size() * 2];
        int i = 0;
        for (String p : readPaths) {
            readPathsCheckList[i++] = p;
            readPathsCheckList[i++] = p + '/';
        }

        userTrees = new HashMap<String, Tree>(principals.size());
        groupTrees = new HashMap<String, Tree>(principals.size());
        if (permissionsTree.exists()) {
            for (Principal principal : principals) {
                Tree t = getPrincipalRoot(permissionsTree, principal);
                Map<String, Tree> target = getTargetMap(principal);
                if (t.exists()) {
                    target.put(principal.getName(), t);
                } else {
                    target.remove(principal.getName());
                }
            }
        }

        userStore = new PermissionStore(userTrees, restrictionProvider);
        groupStore = new PermissionStore(groupTrees, restrictionProvider);
    }

    //------------------------------------------------< CompiledPermissions >---
    @Override
    public void refresh(@Nonnull ImmutableTree permissionsTree,
                        @Nonnull PrivilegeBitsProvider bitsProvider) {
        this.bitsProvider = bitsProvider;
        // test if a permission has been added for those principals that didn't have one before
        for (Principal principal : principals) {
            Map<String, Tree> target = getTargetMap(principal);
            Tree principalRoot = getPrincipalRoot(permissionsTree, principal);
            String pName = principal.getName();
            if (principalRoot.exists()) {
                if (!target.containsKey(pName) || !principalRoot.equals(target.get(pName))) {
                    target.put(pName, principalRoot);
                }
            } else {
                target.remove(pName);
            }
        }
        // todo, improve flush stores
        userStore.flush();
        groupStore.flush();
    }

    @Override
    public ReadStatus getReadStatus(@Nonnull Tree tree, @Nullable PropertyState property) {
        if (isReadablePath(tree, null)) {
            return ReadStatus.ALLOW_ALL_REGULAR;
        }
        long permission = (property == null) ? Permissions.READ_NODE : Permissions.READ_PROPERTY;
        Iterator<PermissionStore.PermissionEntry> it = getEntryIterator(new PermissionStore.EntryPredicate(tree, property));
        while (it.hasNext()) {
            PermissionStore.PermissionEntry entry = it.next();
            if (entry.readStatus != null) {
                return entry.readStatus;
            } else if (entry.privilegeBits.includesRead(permission)) {
                return (entry.isAllow) ? ReadStatus.ALLOW_THIS : ReadStatus.DENY_THIS;
            }
        }
        return ReadStatus.DENY_THIS;
    }

    @Override
    public boolean isGranted(long permissions) {
        return hasPermissions(getEntryIterator(new PermissionStore.EntryPredicate()), permissions, null, null);
    }

    @Override
    public boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
        Iterator<PermissionStore.PermissionEntry> it = getEntryIterator(new PermissionStore.EntryPredicate(tree, property));
        return hasPermissions(it, permissions, tree, null);
    }

    @Override
    public boolean isGranted(@Nonnull String path, long permissions) {
        Iterator<PermissionStore.PermissionEntry> it = getEntryIterator(new PermissionStore.EntryPredicate(path));
        return hasPermissions(it, permissions, null, path);
    }

    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        return bitsProvider.getPrivilegeNames(getPrivilegeBits(tree));
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, String... privilegeNames) {
        return getPrivilegeBits(tree).includes(bitsProvider.getBits(privilegeNames));
    }

    //------------------------------------------------------------< private >---
    @Nonnull
    private static Tree getPrincipalRoot(Tree permissionsTree, Principal principal) {
        return permissionsTree.getChild(Text.escapeIllegalJcrChars(principal.getName()));
    }

    @Nonnull
    private Map<String, Tree> getTargetMap(Principal principal) {
        return (principal instanceof Group) ? groupTrees : userTrees;
    }

    private boolean hasPermissions(@Nonnull Iterator<PermissionStore.PermissionEntry> entries,
                                   long permissions, @Nullable Tree tree, @Nullable String path) {
        // calculate readable paths if the given permissions includes any read permission.
        boolean isReadable = Permissions.diff(Permissions.READ, permissions) != Permissions.READ && isReadablePath(tree, path);
        if (!entries.hasNext() && !isReadable) {
            return false;
        }

        boolean respectParent = (tree != null || path != null) &&
                (Permissions.includes(permissions, Permissions.ADD_NODE) ||
                        Permissions.includes(permissions, Permissions.REMOVE_NODE) ||
                        Permissions.includes(permissions, Permissions.MODIFY_CHILD_NODE_COLLECTION));

        long allows = (isReadable) ? Permissions.READ : Permissions.NO_PERMISSION;
        long denies = Permissions.NO_PERMISSION;

        PrivilegeBits allowBits = PrivilegeBits.getInstance();
        if (isReadable) {
            allowBits.add(bitsProvider.getBits(PrivilegeConstants.JCR_READ));
        }
        PrivilegeBits denyBits = PrivilegeBits.getInstance();
        PrivilegeBits parentAllowBits;
        PrivilegeBits parentDenyBits;
        String parentPath;

        if (respectParent) {
            parentAllowBits = PrivilegeBits.getInstance();
            parentDenyBits = PrivilegeBits.getInstance();
            parentPath = PermissionUtil.getParentPathOrNull((path != null) ? path : tree.getPath());
        } else {
            parentAllowBits = PrivilegeBits.EMPTY;
            parentDenyBits = PrivilegeBits.EMPTY;
            parentPath = null;
        }

        while (entries.hasNext()) {
            PermissionStore.PermissionEntry entry = entries.next();
            if (respectParent && (parentPath != null)) {
                boolean matchesParent = entry.matchesParent(parentPath);
                if (matchesParent) {
                    if (entry.isAllow) {
                        parentAllowBits.addDifference(entry.privilegeBits, parentDenyBits);
                    } else {
                        parentDenyBits.addDifference(entry.privilegeBits, parentAllowBits);
                    }
                }
            }

            if (entry.isAllow) {
                allowBits.addDifference(entry.privilegeBits, denyBits);
                long ap = PrivilegeBits.calculatePermissions(allowBits, parentAllowBits, true);
                allows |= Permissions.diff(ap, denies);
                if ((allows | ~permissions) == -1) {
                    return true;
                }
            } else {
                denyBits.addDifference(entry.privilegeBits, allowBits);
                long dp = PrivilegeBits.calculatePermissions(denyBits, parentDenyBits, false);
                denies |= Permissions.diff(dp, allows);
                if (Permissions.includes(denies, permissions)) {
                    return false;
                }
            }
        }

        return (allows | ~permissions) == -1;
    }

    @Nonnull
    private PrivilegeBits getPrivilegeBits(@Nullable Tree tree) {
        PermissionStore.EntryPredicate pred = (tree == null)
                ? new PermissionStore.EntryPredicate()
                : new PermissionStore.EntryPredicate(tree, null);
        Iterator<PermissionStore.PermissionEntry> entries = getEntryIterator(pred);

        PrivilegeBits allowBits = PrivilegeBits.getInstance();
        PrivilegeBits denyBits = PrivilegeBits.getInstance();

        while (entries.hasNext()) {
            PermissionStore.PermissionEntry entry = entries.next();
            if (entry.isAllow) {
                allowBits.addDifference(entry.privilegeBits, denyBits);
            } else {
                denyBits.addDifference(entry.privilegeBits, allowBits);
            }
        }

        // special handling for paths that are always readable
        if (isReadablePath(tree, null)) {
            allowBits.add(bitsProvider.getBits(PrivilegeConstants.JCR_READ));
        }
        return allowBits;
    }

    @Nonnull
    private Iterator<PermissionStore.PermissionEntry> getEntryIterator(@Nonnull PermissionStore.EntryPredicate predicate) {
        Iterator<PermissionStore.PermissionEntry> userEntries = userStore.getEntryIterator(predicate);
        Iterator<PermissionStore.PermissionEntry> groupEntries = groupStore.getEntryIterator(predicate);
        return Iterators.concat(userEntries, groupEntries);
    }

    private boolean isReadablePath(@Nullable Tree tree, @Nullable String treePath) {
        if (readPathsCheckList.length > 0) {
            String targetPath = (tree != null) ? tree.getPath() : treePath;
            if (targetPath != null) {
                for (int i = 0; i < readPathsCheckList.length; i++) {
                    if (targetPath.equals(readPathsCheckList[i++])) {
                        return true;
                    }
                    if (targetPath.startsWith(readPathsCheckList[i])) {
                        return true;
                    }
                }
            }
        }
        return false;
    }


}
