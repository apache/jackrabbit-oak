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

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code PasswordValidationAction} provides a simple password validation
 * mechanism with the following configurable option:
 *
 * <ul>
 *     <li><strong>constraint</strong>: a regular expression that can be compiled
 *     to a {@link java.util.regex.Pattern} defining validation rules for a password.</li>
 * </ul>
 *
 * <p>The password validation is executed on user creation and upon password
 * change. It throws a {@code ConstraintViolationException} if the password
 * validation fails.</p>
 *
 * @see org.apache.jackrabbit.api.security.user.UserManager#createUser(String, String)
 * @see org.apache.jackrabbit.api.security.user.User#changePassword(String)
 * @see org.apache.jackrabbit.api.security.user.User#changePassword(String, String)
 */
public class PasswordValidationAction extends AbstractAuthorizableAction {

    private static final Logger log = LoggerFactory.getLogger(PasswordValidationAction.class);

    public static final String CONSTRAINT = "constraint";

    private Pattern pattern;

    //-------------------------------------------------< AuthorizableAction >---
    @Override
    public void init(SecurityProvider securityProvider, ConfigurationParameters config) {
        String constraint = config.getConfigValue(CONSTRAINT, null, String.class);
        if (constraint != null) {
            setConstraint(constraint);
        }
    }

    @Override
    public void onCreate(@Nonnull User user, @Nullable String password, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
        validatePassword(password, false);
    }

    @Override
    public void onPasswordChange(@Nonnull User user, @Nullable String newPassword, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
        validatePassword(newPassword, true);
    }

    //------------------------------------------------------------< private >---
    /**
     * Set the password constraint.
     *
     * @param constraint A regular expression that can be used to validate a new password.
     */
    private void setConstraint(@Nonnull String constraint) {
        try {
            pattern = Pattern.compile(constraint);
        } catch (PatternSyntaxException e) {
            log.warn("Invalid password constraint: {}", e.getMessage());
        }
    }

    /**
     * Validate the specified password.
     *
     * @param password The password to be validated
     * @param forceMatch If true the specified password is always validated;
     * otherwise only if it is a plain text password.
     * @throws RepositoryException If the specified password is too short or
     * doesn't match the specified password pattern.
     */
    private void validatePassword(@Nullable String password, boolean forceMatch) throws RepositoryException {
        if (password != null && (forceMatch || PasswordUtil.isPlainTextPassword(password))) {
            if (pattern != null && !pattern.matcher(password).matches()) {
                throw new ConstraintViolationException("Password violates password constraint (" + pattern.pattern() + ").");
            }
        }
    }
}
