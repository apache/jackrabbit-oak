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

import javax.jcr.RepositoryException;

/**
 * The {@code GroupAction} interface allows for implementations to be informed about and react to the following
 * changes to a {@link Group}'s members:
 *
 * <ul>
 * <li>{@link #onMemberAdded(Group, Authorizable, Root, NamePathMapper)}</li>
 * <li>{@link #onMembersAdded(Group, Iterable, Iterable, Root, NamePathMapper)}</li>
 * <li>{@link #onMembersAddedContentId(Group, Iterable, Iterable, Root, NamePathMapper)} </li>
 * <li>{@link #onMemberRemoved(Group, Authorizable, Root, NamePathMapper)}</li>
 * <li>{@link #onMembersRemoved(Group, Iterable, Iterable, Root, NamePathMapper)}</li>
 * </ul>
 *
 * <p>
 * Please consult the parent interface {@link AuthorizableAction} for details on persisting changes,
 * configuring actions and the API through which actions are invoked.
 * </p>
 *
 * <p>
 * For convenience, an {@link AbstractGroupAction} is provided.
 * </p>
 * @since OAK 1.6
 */
public interface GroupAction extends AuthorizableAction {

    /**
     * A specific {@link Authorizable} was added as a member of the {@link Group}.
     * Implementations may perform specific modifications or validations.
     *
     * @param group          The {@link Group} to which the {@link Authorizable} was added.
     * @param member         The {@link Authorizable} added.
     * @param root           The root associated with the user manager.
     * @param namePathMapper
     * @throws RepositoryException If an error occurs.
     */
    void onMemberAdded(Group group, Authorizable member, Root root, NamePathMapper namePathMapper) throws RepositoryException;

    /**
     * Multiple members were added to the {@link Group}. The members are provided as an iterable
     * of their string-based IDs, as some members may no longer or not yet exist.
     * Implementations may perform specific modifications or validations.
     *
     * @param group          The {@link Group} to which the members were added.
     * @param memberIds      An {@link Iterable} of the member IDs.
     * @param root           The root associated with the user manager.
     * @param namePathMapper
     * @throws RepositoryException If an error occurs.
     */
    void onMembersAdded(Group group, Iterable<String> memberIds, Iterable<String> failedIds, Root root, NamePathMapper namePathMapper) throws RepositoryException;

    /**
     * Multiple members were added to the {@link Group} during XML group import.
     * The members are provided as an iterable of their string-based content IDs (UUIDs), as these
     * members do not exist yet (group imported before users). Implementations may track such content ids
     * for later processing once the user identified by the content id is added.
     * <p>
     * Implementations may perform specific modifications or validations.
     *
     * @param group            The {@link Group} to which the members were added.
     * @param memberContentIds An {@link Iterable} of the member content IDs (UUIDs).
     * @param root             The root associated with the user manager.
     * @param namePathMapper
     * @throws RepositoryException If an error occurs.
     */
    void onMembersAddedContentId(Group group, Iterable<String> memberContentIds, Iterable<String> failedIds, Root root, NamePathMapper namePathMapper) throws RepositoryException;

    /**
     * A specific {@link Authorizable} was removed from the {@link Group}.
     * Implementations may perform specific modifications or validations.
     *
     * @param group          The {@link Group} from which the {@link Authorizable} was removed.
     * @param member         The {@link Authorizable} removed.
     * @param root           The root associated with the user manager.
     * @param namePathMapper
     * @throws RepositoryException If an error occurs.
     */
    void onMemberRemoved(Group group, Authorizable member, Root root, NamePathMapper namePathMapper) throws RepositoryException;

    /**
     * Multiple members were removed from the {@link Group}. The members are provided as an iterable
     * of their string-based IDs, as some members may no longer or not yet exist.
     * Implementations may perform specific modifications or validations.
     *
     * @param group          The {@link Group} from which the members were removed.
     * @param memberIds      An {@link Iterable} of the member IDs.
     * @param root           The root associated with the user manager.
     * @param namePathMapper
     * @throws RepositoryException If an error occurs.
     */
    void onMembersRemoved(Group group, Iterable<String> memberIds, Iterable<String> failedIds, Root root, NamePathMapper namePathMapper) throws RepositoryException;
}
