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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

/**
 * Abstract implementation of the {@code GroupAction} interface that
 * doesn't perform any action. This is a convenience implementation allowing
 * subclasses to only implement methods that need extra attention.
 */
public abstract class AbstractGroupAction extends AbstractAuthorizableAction implements GroupAction {

    //-------------------------------------------------< GroupAction >---

    /**
     * Doesn't perform any action.
     */
    @Override
    public void onMemberAdded(@Nonnull Group group, @Nonnull Authorizable member, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
        // nothing to do
    }

    /**
     * Doesn't perform any action.
     */
    @Override
    public void onMembersAdded(@Nonnull Group group, @Nonnull Iterable<String> memberIds, @Nonnull Iterable<String> failedIds, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
        // nothing to do
    }

    /**
     * Doesn't perform any action.
     */
    @Override
    public void onMembersAddedContentId(Group group, Iterable<String> memberContentIds, Iterable<String> failedIds, Root root, NamePathMapper namePathMapper) throws RepositoryException {
        // nothing to do
    }

    /**
     * Doesn't perform any action.
     */
    @Override
    public void onMemberRemoved(@Nonnull Group group, @Nonnull Authorizable member, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
        // nothing to do
    }

    /**
     * Doesn't perform any action.
     */
    @Override
    public void onMembersRemoved(@Nonnull Group group, @Nonnull Iterable<String> memberIds, @Nonnull Iterable<String> failedIds, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
        // nothing to do
    }
}
