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

import java.security.Principal;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;

/**
 * Default implementation for a system user.
 */
class SystemUserImpl extends UserImpl {

    SystemUserImpl(@Nonnull String id, @Nonnull Tree tree, @Nonnull UserManagerImpl userManager) throws RepositoryException {
        super(id, tree, userManager);
    }

    @Override
    void checkValidTree(@Nonnull Tree tree) throws RepositoryException {
        super.checkValidTree(tree);
        if (!UserUtil.isSystemUser(tree)) {
            throw new IllegalArgumentException("Invalid user node: node type rep:SystemUser expected.");
        }
    }

    @Override
    public Principal getPrincipal() throws RepositoryException {
        if (isAdmin()) {
            return new AdminPrincipalImpl(getPrincipalName(), getTree(), getUserManager().getNamePathMapper());
        } else {
            return new SystemUserPrincipalImpl(getPrincipalName(), getTree(), getUserManager().getNamePathMapper());
        }
    }

    //---------------------------------------------------------------< User >---
    @Override
    public boolean isSystemUser() {
        return true;
    }

    @Override
    public void changePassword(String password) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("system user");
    }

    @Override
    public void changePassword(String password, String oldPassword) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("system user");
    }
}