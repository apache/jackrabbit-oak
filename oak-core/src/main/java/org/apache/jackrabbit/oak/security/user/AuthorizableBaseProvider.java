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

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.UUIDUtils.generateUUID;
import static org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager.getIdentifier;

/**
 * Base class for {@link UserProvider} and {@link MembershipProvider}.
 */
abstract class AuthorizableBaseProvider implements UserConstants {

    final ConfigurationParameters config;
    final Root root;
    final IdentifierManager identifierManager;

    private final boolean usercaseMappedProfile;

    AuthorizableBaseProvider(@NotNull Root root, @NotNull ConfigurationParameters config) {
        this.root = checkNotNull(root);
        this.config = checkNotNull(config);

        identifierManager = new IdentifierManager(root);
        usercaseMappedProfile = config.getConfigValue(PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE, DEFAULT_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE);
    }

    @Nullable
    Tree getByID(@NotNull String authorizableId, @NotNull AuthorizableType authorizableType) {
        return getByContentID(getContentID(authorizableId), authorizableType);
    }

    @Nullable
    Tree getByContentID(@NotNull String contentId, @NotNull AuthorizableType authorizableType) {
        Tree tree = identifierManager.getTree(contentId);
        if (UserUtil.isType(tree, authorizableType)) {
            return tree;
        } else {
            return null;
        }
    }

    @Nullable
    Tree getByPath(@NotNull String authorizableOakPath, @NotNull AuthorizableType type) {
        Tree tree = root.getTree(authorizableOakPath);
        if (UserUtil.isType(tree, type)) {
            return tree;
        } else {
            return null;
        }
    }

    @NotNull
    String getContentID(@NotNull Tree authorizableTree) {
        return getIdentifier(authorizableTree);
    }

    @NotNull
    String getContentID(@NotNull String authorizableId) {
        String s = authorizableId.toLowerCase();
        if (usercaseMappedProfile) {
            s = java.text.Normalizer.normalize(s, java.text.Normalizer.Form.NFKC);
        }
        return UUIDUtils.generateUUID(s);
    }
}
