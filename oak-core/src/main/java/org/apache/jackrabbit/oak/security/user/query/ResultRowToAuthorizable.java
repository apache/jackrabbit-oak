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
package org.apache.jackrabbit.oak.security.user.query;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import com.google.common.base.Function;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ResultRowToAuthorizable... TODO
 */
class ResultRowToAuthorizable implements Function<ResultRow, Authorizable> {

    private static final Logger log = LoggerFactory.getLogger(ResultRowToAuthorizable.class);

    private final UserManager userManager;
    private final Root root;
    private final AuthorizableType targetType;

    ResultRowToAuthorizable(@Nonnull UserManager userManager, @Nonnull Root root,
                            @Nullable AuthorizableType targetType) {
        this.userManager = userManager;
        this.root = root;
        this.targetType = (targetType == null || AuthorizableType.AUTHORIZABLE == targetType) ? null : targetType;
    }

    @Override
    public Authorizable apply(ResultRow row) {
        if (row != null) {
            return getAuthorizable(row.getPath());
        }
        return null;
    }

    //------------------------------------------------------------< private >---
    @CheckForNull
    private Authorizable getAuthorizable(String resultPath) {
        Authorizable authorizable = null;
        try {
            Tree tree = root.getTree(resultPath);
            AuthorizableType type = UserUtility.getType(tree);
            while (tree != null && type == null) {
                tree = tree.getParent();
                type = UserUtility.getType(tree);
            }
            if (type != null && (targetType == null || targetType == type)) {
                authorizable = userManager.getAuthorizableByPath(tree.getPath());
            }
        } catch (RepositoryException e) {
            log.debug("Failed to access authorizable " + resultPath);
        }
        return authorizable;
    }
}