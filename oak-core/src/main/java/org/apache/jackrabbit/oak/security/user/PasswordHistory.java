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

import java.util.ArrayList;
import java.util.List;
import javax.jcr.AccessDeniedException;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.jetbrains.annotations.NotNull;

/**
 * Helper class for the password history feature.
 */
final class PasswordHistory implements UserConstants {

    private static final int HISTORY_MAX_SIZE = 1000;

    private final int maxSize;
    private final boolean isEnabled;

    PasswordHistory(@NotNull ConfigurationParameters config) {
        maxSize = Math.min(HISTORY_MAX_SIZE, config.getConfigValue(UserConstants.PARAM_PASSWORD_HISTORY_SIZE, UserConstants.PASSWORD_HISTORY_DISABLED_SIZE));
        isEnabled = maxSize > UserConstants.PASSWORD_HISTORY_DISABLED_SIZE;
    }

    /**
     * If password history is enabled this method validates the new password and
     * updated the history; otherwise it returns {@code false}.
     *
     * @param userTree The user tree.
     * @param password The new password to be validated.
     * @return {@code true} if the history is enabled, the new password is not
     * included in the history and the history was successfully updated;
     * {@code false} otherwise.
     * @throws javax.jcr.nodetype.ConstraintViolationException If the feature
     * is enabled and the new password is found in the history.
     * @throws javax.jcr.AccessDeniedException If the rep:pwd tree cannot be
     * accessed.
     */
    boolean updatePasswordHistory(@NotNull Tree userTree, @NotNull String password) throws ConstraintViolationException, AccessDeniedException {
        boolean updated = false;
        if (isEnabled) {
            checkPasswordInHistory(userTree, password);
            updated = shiftPasswordHistory(userTree);
        }
        return updated;
    }

    /**
     * Update the history property with the current pw-hash stored in rep:password
     * and trim the list of hashes in the list according to the configured maxSize.
     *
     * @param userTree The user tree.
     * @return true if the history was successfully adjusted, false otherwise
     * @throws AccessDeniedException If the editing session cannot access or
     * create the rep:pwd node.
     */
    private boolean shiftPasswordHistory(@NotNull Tree userTree) throws AccessDeniedException {
        String currentPasswordHash = TreeUtil.getString(userTree, UserConstants.REP_PASSWORD);
        if (currentPasswordHash != null) {
            Tree passwordTree = getPasswordTree(userTree, true);
            PropertyState historyProp = passwordTree.getProperty(UserConstants.REP_PWD_HISTORY);

            // insert the current (old) password at the beginning of the password history
            List<String> historyEntries = (historyProp == null) ? new ArrayList<>() : CollectionUtils.toList(historyProp.getValue(Type.STRINGS));
            historyEntries.add(0, currentPasswordHash);

            /* remove oldest history entries exceeding configured history max size (e.g. after
             * a configuration change)
             */
            if (historyEntries.size() > maxSize) {
                historyEntries = historyEntries.subList(0, maxSize);
            }

            passwordTree.setProperty(UserConstants.REP_PWD_HISTORY, historyEntries, Type.STRINGS);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Verify that the specified new password is not contained in the history.
     *
     * @param userTree The user tree.
     * @param newPassword The new password
     * @throws ConstraintViolationException If the passsword is found in the history
     * @throws AccessDeniedException If the editing session cannot access the rep:pwd node.
     */
    private void checkPasswordInHistory(@NotNull Tree userTree, @NotNull String newPassword) throws ConstraintViolationException, AccessDeniedException {
        if (PasswordUtil.isSame(TreeUtil.getString(userTree, UserConstants.REP_PASSWORD), newPassword)) {
            throw new PasswordHistoryException("New password is identical to the current password.");
        }
        Tree pwTree = getPasswordTree(userTree, false);
        if (pwTree.exists()) {
            PropertyState pwHistoryProperty = pwTree.getProperty(UserConstants.REP_PWD_HISTORY);
            if (pwHistoryProperty != null) {
                for (String historyPwHash : Iterables.limit(pwHistoryProperty.getValue(Type.STRINGS), maxSize)) {
                    if (PasswordUtil.isSame(historyPwHash, newPassword)) {
                        throw new PasswordHistoryException("New password was found in password history.");
                    }
                }
            }
        }
    }

    @NotNull
    private static Tree getPasswordTree(@NotNull Tree userTree, boolean doCreate) throws AccessDeniedException {
        if (doCreate) {
            return TreeUtil.getOrAddChild(userTree, UserConstants.REP_PWD, UserConstants.NT_REP_PASSWORD);
        } else {
            return userTree.getChild(UserConstants.REP_PWD);
        }
    }
}
