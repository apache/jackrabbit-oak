package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.principal.GroupPrincipals;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class AutoMembershipPrincipals {

    private static final Logger log = LoggerFactory.getLogger(AutoMembershipPrincipals.class);

    private final UserManager userManager;
    private final Map<String, String[]> autoMembershipMapping;
    private final Map<String, Set<Principal>> principalMap;

    AutoMembershipPrincipals(@NotNull UserManager userManager, @NotNull Map<String, String[]> autoMembershipMapping) {
        this.userManager = userManager;
        this.autoMembershipMapping = autoMembershipMapping;
        this.principalMap = new ConcurrentHashMap<>(autoMembershipMapping.size());
    }

    @NotNull
    Collection<Principal> getPrincipals(@Nullable String idpName) {
        if (idpName == null) {
            return ImmutableSet.of();
        }

        Set<Principal> principals;
        if (!principalMap.containsKey(idpName)) {
            principals = collectAutomembershipPrincipals(idpName);
            principalMap.put(idpName, principals);
        } else {
            principals = principalMap.get(idpName);
        }
        return principals;
    }

    @NotNull
    private Set<Principal> collectAutomembershipPrincipals(@NotNull String idpName) {
        String[] vs = autoMembershipMapping.get(idpName);
        if (vs == null) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<Principal> builder = ImmutableSet.builder();
        for (String groupId : vs) {
            try {
                Authorizable gr = userManager.getAuthorizable(groupId);
                if (gr != null && gr.isGroup()) {
                    Principal grPrincipal = gr.getPrincipal();
                    if (GroupPrincipals.isGroup(grPrincipal)) {
                        builder.add(grPrincipal);
                    } else {
                        log.warn("Principal of group {} is not of group type -> Ignoring", groupId);
                    }
                } else {
                    log.warn("Configured auto-membership group {} does not exist -> Ignoring", groupId);
                }
            } catch (RepositoryException e) {
                log.debug("Failed to retrieved 'auto-membership' group with id {}", groupId, e);
            }
        }
        return builder.build();
    }
}
