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

import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;

/**
 * The {@code AuthorizableAction} interface provide an implementation
 * specific way to execute additional validation or write tasks upon
 *
 * <ul>
 * <li>{@link #onCreate(org.apache.jackrabbit.api.security.user.User, String, javax.jcr.Session) User creation},</li>
 * <li>{@link #onCreate(org.apache.jackrabbit.api.security.user.Group, javax.jcr.Session) Group creation},</li>
 * <li>{@link #onRemove(org.apache.jackrabbit.api.security.user.Authorizable, javax.jcr.Session) Authorizable removal} and</li>
 * <li>{@link #onPasswordChange(org.apache.jackrabbit.api.security.user.User, String, javax.jcr.Session) User password modification}.</li>
 * </ul>
 *
 * @see org.apache.jackrabbit.oak.spi.security.user.UserConfig
 */
public interface AuthorizableAction {

    /**
     * Allows to add application specific modifications or validation associated
     * with the creation of a new group. Note, that this method is called
     * <strong>before</strong> any {@code Session.save} call.
     *
     * @param group The new group that has not yet been persisted;
     * e.g. the associated node is still 'NEW'.
     * @param session The editing session associated with the user manager.
     * @throws javax.jcr.RepositoryException If an error occurs.
     */
    void onCreate(Group group, Session session) throws RepositoryException;

    /**
     * Allows to add application specific modifications or validation associated
     * with the creation of a new user. Note, that this method is called
     * <strong>before</strong> any {@code Session.save} call.
     *
     * @param user The new user that has not yet been persisted;
     * e.g. the associated node is still 'NEW'.
     * @param password The password that was specified upon user creation.
     * @param session The editing session associated with the user manager.
     * @throws RepositoryException If an error occurs.
     */
    void onCreate(User user, String password, Session session) throws RepositoryException;

    /**
     * Allows to add application specific behavior associated with the removal
     * of an authorizable. Note, that this method is called <strong>before</strong>
     * {@link org.apache.jackrabbit.api.security.user.Authorizable#remove} is executed (and persisted); thus the
     * target authorizable still exists.
     *
     * @param authorizable The authorizable to be removed.
     * @param session The editing session associated with the user manager.
     * @throws RepositoryException If an error occurs.
     */
    void onRemove(Authorizable authorizable, Session session) throws RepositoryException;

    /**
     * Allows to add application specific action or validation associated with
     * changing a user password. Note, that this method is called <strong>before</strong>
     * the password property is being modified in the content.
     *
     * @param user The user that whose password is going to change.
     * @param newPassword The new password as specified in {@link User#changePassword}
     * @param session The editing session associated with the user manager.
     * @throws RepositoryException If an exception or error occurs.
     */
    void onPasswordChange(User user, String newPassword, Session session) throws RepositoryException;
}