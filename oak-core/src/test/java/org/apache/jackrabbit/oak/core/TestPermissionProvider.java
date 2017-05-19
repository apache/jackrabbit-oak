package org.apache.jackrabbit.oak.core;

import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Dummy permission provider implementation that grants read access to all trees
 * that have a name that isn't equal to {@link #NAME_NON_ACCESSIBLE}.
 */
final class TestPermissionProvider implements PermissionProvider {

    static final String NAME_ACCESSIBLE = "accessible";
    static final String NAME_NON_ACCESSIBLE = "notAccessible";
    static final String NAME_NON_EXISTING = "nonExisting";

    boolean canReadAll = false;
    boolean canReadProperties = false;

    boolean denyAll;

    private TreePermission getTreePermission(@Nonnull String name) {
        if (denyAll) {
            return TreePermission.EMPTY;
        } else {
            return new TreePermission() {
                @Nonnull
                @Override
                public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
                    return getTreePermission(childName);
                }

                @Override
                public boolean canRead() {
                    return canReadAll || !name.contains(NAME_NON_ACCESSIBLE);
                }

                @Override
                public boolean canRead(@Nonnull PropertyState property) {
                    return canReadProperties || !property.getName().contains(NAME_NON_ACCESSIBLE);
                }

                @Override
                public boolean canReadAll() {
                    return canReadAll;
                }

                @Override
                public boolean canReadProperties() {
                    return canReadProperties;
                }

                @Override
                public boolean isGranted(long permissions) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean isGranted(long permissions, @Nonnull PropertyState property) {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    @Override
    public void refresh() {
        denyAll = !denyAll;
    }

    @Nonnull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @Nonnull String... privilegeNames) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission) {
        return getTreePermission(tree.getName());
    }

    @Override
    public boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
        throw new UnsupportedOperationException();
    }
}
