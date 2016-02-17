package org.apache.jackrabbit.oak.spi.security.user.action;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;

/**
 * The {@code GroupAction} interface extends {@link AuthorizableAction}
 * with group specific events.
 */
public interface GroupAction extends AuthorizableAction {

    /**
     * Allows to add application specific modifications or validation associated with the
     * added membership of an authorizable. Note, that this method is called
     * <strong>before</strong> any {@code Root#commit()} call.
     *
     * @param group The group the member has been added to
     * @param member The authorizable
     * @param root The root associated with the user manager.
     * @param namePathMapper The oak name mapper.
     * @throws RepositoryException If an error occurs and the change should not be persisted.
     */
    void onMemberAdded(Group group, Authorizable member, Root root, NamePathMapper namePathMapper) throws RepositoryException;

    /**
     * Allows to add application specific modifications or validation associated with the
     * removed membership of an authorizable. Note, that this method is called
     * <strong>before</strong> any {@code Root#commit()} call.
     *
     * @param group The group the member has been removed from
     * @param member The authorizable
     * @param root The root associated with the user manager.
     * @param namePathMapper The oak name mapper.
     * @throws RepositoryException If an error occurs and the change should not be persisted.
     */
    void onMemberRemoved(Group group, Authorizable member, Root root, NamePathMapper namePathMapper) throws RepositoryException;
}
