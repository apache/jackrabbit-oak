package org.apache.jackrabbit.oak.security.user;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.security.user.action.GroupAction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Tests {@link GroupAction} callbacks.
 */
public class GroupActionTest extends AbstractSecurityTest {

    private UserManager userMgr;
    private Collection<String> memberAddedCallbacks = new ArrayList<String>();
    private Collection<String> memberRemovedCallbacks = new ArrayList<String>();

    @Test
    public void testAddMember() throws RepositoryException {
        Group group = userMgr.createGroup("test-group");
        User user = userMgr.createUser("test-user", "");

        group.addMember(user);

        Assert.assertTrue("GroupAction.onMemberAdded() not called", memberAddedCallbacks.contains(membership(group, user)));
    }

    @Test
    public void testAddMembers() throws RepositoryException {
        Group group = userMgr.createGroup("test-group");
        User user = userMgr.createUser("test-user", "");
        User user2 = userMgr.createUser("test-user2", "");

        group.addMembers(user.getID(), user2.getID());

        Assert.assertTrue("GroupAction.onMemberAdded() not called", memberAddedCallbacks.contains(membership(group, user)));
        Assert.assertTrue("GroupAction.onMemberAdded() not called", memberAddedCallbacks.contains(membership(group, user2)));
    }

    @Test
    public void testRemoveMember() throws RepositoryException, CommitFailedException {
        Group group = userMgr.createGroup("test-group");
        User user = userMgr.createUser("test-user", "");
        group.addMember(user);
        root.commit();

        group.removeMember(user);

        Assert.assertTrue("GroupAction.onMemberRemoved() not called", memberRemovedCallbacks.contains(membership(group, user)));
    }

    @Test
    public void testRemoveMembers() throws RepositoryException, CommitFailedException {
        Group group = userMgr.createGroup("test-group");
        User user = userMgr.createUser("test-user", "");
        User user2 = userMgr.createUser("test-user2", "");
        group.addMember(user);
        group.addMember(user2);
        root.commit();

        group.removeMembers(user.getID(), user2.getID());

        Assert.assertTrue("GroupAction.onMemberRemoved() not called", memberRemovedCallbacks.contains(membership(group, user)));
        Assert.assertTrue("GroupAction.onMemberRemoved() not called", memberRemovedCallbacks.contains(membership(group, user2)));
    }

    // ----------------------------------------------------< test utilities >--------------

    @Before
    public void setup() throws Exception {
        userMgr = getUserManager(root);
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(
            UserConfiguration.NAME,
            ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, new AuthorizableActionProvider() {
                @Nonnull
                @Override
                public List<? extends AuthorizableAction> getAuthorizableActions(@Nonnull SecurityProvider securityProvider) {
                    TestGroupAction action = new TestGroupAction();
                    action.init(securityProvider, null);
                    return ImmutableList.of(action);
                }
            }));
    }

    private String membership(Group group, Authorizable member) throws RepositoryException {
        return group.getID() + " -- " + member.getID();
    }

    private class TestGroupAction implements GroupAction {

        @Override
        public void onMemberAdded(Group group, Authorizable member, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            memberAddedCallbacks.add(membership(group, member));
        }

        @Override
        public void onMemberRemoved(Group group, Authorizable member, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            memberRemovedCallbacks.add(membership(group, member));
        }

        @Override
        public void init(SecurityProvider securityProvider, ConfigurationParameters config) {
        }

        @Override
        public void onCreate(Group group, Root root, NamePathMapper namePathMapper) throws RepositoryException {
        }

        @Override
        public void onCreate(User user, String password, Root root, NamePathMapper namePathMapper) throws RepositoryException {
        }

        @Override
        public void onRemove(Authorizable authorizable, Root root, NamePathMapper namePathMapper) throws RepositoryException {
        }

        @Override
        public void onPasswordChange(User user, String newPassword, Root root, NamePathMapper namePathMapper) throws RepositoryException {
        }
    }
}
