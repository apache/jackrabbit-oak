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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.tree.TreeAware;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_ID;

class DynamicGroupUtil {

    private static final Logger log = LoggerFactory.getLogger(DynamicGroupUtil.class);

    private static final Set<String> MEMBER_NODE_NAMES = Set.of(UserConstants.REP_MEMBERS, UserConstants.REP_MEMBERS_LIST);
    private static final Set<String> MEMBERS_TYPES = Set.of(UserConstants.NT_REP_MEMBER_REFERENCES, UserConstants.NT_REP_MEMBER_REFERENCES_LIST, UserConstants.NT_REP_MEMBERS);

    private DynamicGroupUtil() {}

    static boolean isGroup(@NotNull Tree tree) {
        return UserUtil.isType(tree, AuthorizableType.GROUP);
    }

    static boolean isMemberProperty(@NotNull PropertyState propertyState) {
        return UserConstants.REP_MEMBERS.equals(propertyState.getName());
    }

    static @Nullable String findGroupIdInHierarchy(@NotNull Tree tree) {
        Tree t = tree;
        while (!t.isRoot()) {
            String id = UserUtil.getAuthorizableId(t);
            if (id != null) {
                return id;
            }
            t = t.getParent();
        }
        return null;
    }

    @NotNull
    static Tree getTree(@NotNull Authorizable authorizable, @NotNull Root root) throws RepositoryException {
        return (authorizable instanceof TreeAware) ? ((TreeAware) authorizable).getTree() : root.getTree(authorizable.getPath());
    }

    static boolean hasStoredMemberInfo(@NotNull Group group, @NotNull Root root) {
        try {
            Tree tree = getTree(group, root);
            return tree.hasProperty(UserConstants.REP_MEMBERS) || MEMBER_NODE_NAMES.stream().anyMatch(tree::hasChild);
        } catch (RepositoryException e) {
            log.error("Cannot test for stored members information, failed to obtain tree from group.", e);
            return false;
        }
    }

    static boolean isMembersType(@NotNull Tree tree) {
        String primaryType = TreeUtil.getPrimaryTypeName(tree);
        return primaryType != null && MEMBERS_TYPES.contains(primaryType);
    }

    @Nullable
    static String getIdpName(@NotNull Tree userTree) {
        PropertyState ps = userTree.getProperty(REP_EXTERNAL_ID);
        if (ps != null) {
            return ExternalIdentityRef.fromString(ps.getValue(Type.STRING)).getProviderName();
        } else {
            return null;
        }
    }

    @Nullable
    static String getIdpName(@NotNull ResultRow row) {
        return getIdpName(row.getTree(null));
    }

    @Nullable
    static String getIdpName(@NotNull Authorizable authorizable) throws RepositoryException {
        ExternalIdentityRef ref = DefaultSyncContext.getIdentityRef(authorizable);
        return (ref == null) ? null : ref.getProviderName();
    }

    static boolean isSameIDP(@NotNull Authorizable group, @NotNull Authorizable member) throws RepositoryException {
        String groupIdpName = getIdpName(group);
        if (groupIdpName == null) {
            log.warn("Referenced dynamic group '{}' not associated with an external IDP.", group.getID());
            return false; 
        }

        String idpName = getIdpName(member);
        if (groupIdpName.equals(idpName)) {
            return true;
        } else {
            log.warn("IDP mismatch between dynamic group '{}' and member '{}'.", groupIdpName, idpName);
            return false;
        }
    }

    static Set<Principal> getInheritedPrincipals(@NotNull Principal dynamicGroupPrincipal, @NotNull UserManager userManager) {
        try {
            Authorizable gr = userManager.getAuthorizable(dynamicGroupPrincipal);
            if (gr != null && gr.isGroup()) {
                Iterator<Group> inherited = gr.memberOf();
                if (inherited.hasNext()) {
                    Spliterator<Group> spliterator = Spliterators.spliteratorUnknownSize(inherited, 0);
                    return StreamSupport.stream(spliterator, false).map(group -> {
                        try {
                            return group.getPrincipal();
                        } catch (RepositoryException repositoryException) {
                            return null;
                        }
                    }).filter(Objects::nonNull).collect(Collectors.toSet());
                }
            }
        } catch (RepositoryException e) {
            log.error("Failed to retrieve inherited group principals", e);
        }
        return Collections.emptySet();
    }
}