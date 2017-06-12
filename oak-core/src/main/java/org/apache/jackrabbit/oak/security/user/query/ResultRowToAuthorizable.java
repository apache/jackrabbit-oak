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
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.security.user.UserManagerImpl;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Function to convert query result rows {@link Authorizable}s of a given
 * target type.
 */
class ResultRowToAuthorizable implements Function<ResultRow, Authorizable> {

    private static final Logger log = LoggerFactory.getLogger(ResultRowToAuthorizable.class);

    private final UserManagerImpl userManager;
    private final Root root;
    private final AuthorizableType targetType;

    ResultRowToAuthorizable(@Nonnull UserManagerImpl userManager, @Nonnull Root root,
                            @Nullable AuthorizableType targetType) {
        this.userManager = userManager;
        this.root = root;
        this.targetType = (targetType == null || AuthorizableType.AUTHORIZABLE == targetType) ? null : targetType;
    }

    @Nullable
    @Override
    public Authorizable apply(@Nullable ResultRow row) {
        return getAuthorizable(row);
    }

    //------------------------------------------------------------< private >---
    @CheckForNull
    private Authorizable getAuthorizable(@CheckForNull ResultRow row) {
        Authorizable authorizable = null;
        if (row != null) {
            String resultPath = row.getValue(QueryConstants.JCR_PATH).getValue(Type.STRING);
            try {
                Tree tree = root.getTree(resultPath);
                AuthorizableType type = UserUtil.getType(tree);
                while (tree.exists() && !tree.isRoot() && type == null) {
                    tree = tree.getParent();
                    type = UserUtil.getType(tree);
                }
                if (tree.exists() && (targetType == null || targetType == type)) {
                    authorizable = userManager.getAuthorizable(tree);
                }
            } catch (RepositoryException e) {
                log.debug("Failed to access authorizable " + resultPath);
            }
        }
        return authorizable;
    }
}