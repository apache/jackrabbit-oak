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
import org.apache.jackrabbit.oak.api.Root;

/**
 * Abstract implementation of the {@code AuthorizableAction} interface that
 * doesn't perform any action. This is a convenience implementation allowing
 * subclasses to only implement methods that need extra attention.
 */
public abstract class AbstractAuthorizableAction implements AuthorizableAction {

    /**
     * Doesn't perform any action.
     *
     * @see AuthorizableAction#onCreate(org.apache.jackrabbit.api.security.user.Group, javax.jcr.Session)
     */
    @Override
    public void onCreate(Group group, Session session) throws RepositoryException {
        // nothing to do

    }

    /**
     * Doesn't perform any action.
     *
     * @see AuthorizableAction#onCreate(org.apache.jackrabbit.api.security.user.Group, Root)
     */
    @Override
    public void onCreate(Group group, Root root) throws RepositoryException {
        // nothing to do
    }

    /**
     * Doesn't perform any action.
     *
     * @see AuthorizableAction#onCreate(org.apache.jackrabbit.api.security.user.User, String, javax.jcr.Session)
     */
    @Override
    public void onCreate(User user, String password, Session session) throws RepositoryException {
        // nothing to do
    }

    /**
     * Doesn't perform any action.
     *
     * @see AuthorizableAction#onCreate(org.apache.jackrabbit.api.security.user.User, String, Root)
     */
    @Override
    public void onCreate(User user, String password, Root root) throws RepositoryException {
        // nothing to do
    }

    /**
     * Doesn't perform any action.
     *
     * @see AuthorizableAction#onRemove(org.apache.jackrabbit.api.security.user.Authorizable, javax.jcr.Session)
     */
    @Override
    public void onRemove(Authorizable authorizable, Session session) throws RepositoryException {
        // nothing to do
    }

    /**
     * Doesn't perform any action.
     *
     * @see AuthorizableAction#onRemove(org.apache.jackrabbit.api.security.user.Authorizable, Root)
     */
    @Override
    public void onRemove(Authorizable authorizable, Root root) throws RepositoryException {
        // nothing to do
    }

    /**
     * Doesn't perform any action.
     *
     * @see AuthorizableAction#onPasswordChange(org.apache.jackrabbit.api.security.user.User, String, javax.jcr.Session)
     */
    @Override
    public void onPasswordChange(User user, String newPassword, Session session) throws RepositoryException {
        // nothing to do
    }

    /**
     * Doesn't perform any action.
     *
     * @see AuthorizableAction#onPasswordChange(org.apache.jackrabbit.api.security.user.User, String, Root)
     */
    @Override
    public void onPasswordChange(User user, String newPassword, Root root) throws RepositoryException {
        // nothing to do
    }
}