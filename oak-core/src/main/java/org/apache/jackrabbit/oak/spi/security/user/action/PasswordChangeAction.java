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
package org.apache.jackrabbit.oak.spi.security.user.action;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;

/**
 * {@code PasswordChangeAction} asserts that the upon
 * {@link #onPasswordChange(org.apache.jackrabbit.api.security.user.User, String,
 * org.apache.jackrabbit.oak.api.Root, org.apache.jackrabbit.oak.namepath.NamePathMapper)}
 * a different, non-null password is specified.
 *
 * @see org.apache.jackrabbit.api.security.user.User#changePassword(String)
 * @see org.apache.jackrabbit.api.security.user.User#changePassword(String, String)
 *
 * @since OAK 1.0
 */
public class PasswordChangeAction extends AbstractAuthorizableAction {

    //-------------------------------------------------< AuthorizableAction >---
    @Override
    public void onPasswordChange(@Nonnull User user, String newPassword, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
        if (newPassword == null) {
            throw new ConstraintViolationException("Expected a new password that is not null.");
        }
        String pwHash = getPasswordHash(root, user);
        if (PasswordUtil.isSame(pwHash, newPassword)) {
            throw new ConstraintViolationException("New password is identical to the old password.");
        }
    }

    //------------------------------------------------------------< private >---
    @CheckForNull
    private String getPasswordHash(@Nonnull Root root, @Nonnull User user) throws RepositoryException {
        return TreeUtil.getString(root.getTree(user.getPath()), UserConstants.REP_PASSWORD);
    }
}