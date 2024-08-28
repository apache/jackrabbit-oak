package org.apache.jackrabbit.oak.security.user;

import java.security.Principal;
import java.util.Set;
import org.apache.jackrabbit.oak.api.Tree;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface PrincipalMembershipReader {

    Set<Principal> readMembership(@NotNull Tree authorizableTree);

    interface GroupPrincipalFactory {

        @Nullable
        Principal create(@NotNull Tree authorizable);

        @NotNull
        Principal create(@NotNull String principalName);
    }
}
