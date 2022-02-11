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

import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NODE_TYPES_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.util.Iterator;
import java.util.UUID;

import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AbstractAuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AccessControlAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * OAK-9675 - Demonstrates how to use a combination of ACLs, mixin and primary
 * type configuration to make it safe to allow end users to alter their own
 * profile properties but not add new properties.  Also demonstrates how
 * value constraints on the property definitions can limit what values
 * may be stored.
 */
public class Oak9765AuthorizablePropertiesTest extends AbstractSecurityTest {

    private static final String INITIAL_EMAIL_VALUE = "test@example.com";
    private static final String PROP_PRIVATE_EMAIL = "private/email";
    private static final String PROP_DISPLAYNAME = "displayName";

    private User testUser2;
    private Group testGroup;
    private ContentSession user1Session;
    private ContentSession user2Session;

    /**
     * Override to provide the configuration for this scenario
     */
    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(UserConfiguration.NAME,
                ConfigurationParameters.of(
                    // configure the mixin type that declares extra properties that are visible
                    // for users and groups
                    ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_PROPERTIES_MIXINS, new String[] {
                            "oak9765:userPublic",
                            "oak9765:groupPublic"
                        }),

                    // configure where the users and groups are stored
                    ConfigurationParameters.of(UserConstants.PARAM_USER_PATH, "/home/users"),
                    ConfigurationParameters.of(UserConstants.PARAM_GROUP_PATH, "/home/groups"),

                    // configure the action providers to do extra work when a user or
                    // group is created
                    ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, (AuthorizableActionProvider) securityProvider -> {
                        // configure the privileges granted to a user or group when it is created
                        //   only "read" and "alter properties" rights for the user
                        //   only "read" rights for the group
                        AuthorizableAction accessControlAction = new AccessControlAction();
                        accessControlAction.init(securityProvider, 
                                ConfigurationParameters.of(
                                    ConfigurationParameters.of(AccessControlAction.USER_PRIVILEGE_NAMES, 
                                            new String[]{
                                                    PrivilegeConstants.JCR_READ,
                                                    PrivilegeConstants.REP_ALTER_PROPERTIES
                                             }
                                    ),
                                    ConfigurationParameters.of(AccessControlAction.GROUP_PRIVILEGE_NAMES, 
                                            new String[]{
                                                    PrivilegeConstants.JCR_READ
                                             }
                                    )
                                )
                            );

                        // action to pre-create the "public" user-modifiable properties and declare the mixin type
                        AuthorizableAction publicProfileAction = new PublicProfileAction();
                        publicProfileAction.init(securityProvider, ConfigurationParameters.EMPTY);

                        // action to pre-create the "private" subnode and the user-modifiable properties
                        AuthorizableAction privateProfileAction = new PrivateProfileAction();
                        privateProfileAction.init(securityProvider, ConfigurationParameters.EMPTY);

                        return ImmutableList.of(accessControlAction, publicProfileAction, privateProfileAction);
                    })
                )
            );
    }

    /**
     * Override to:
     * 1. register custom node types
     * 2. grant everyone permissions to read the /home/users folder
     * 3. create some test users and groups
     */
    @Override
    public void before() throws Exception {
        super.before();

        // register our custom node types
        try (InputStream is = getClass().getResourceAsStream("oak9675.cnd")) {
            NodeTypeRegistry.register(root, is, Oak9765AuthorizablePropertiesTest.class.getName());
        }

        // grant everyone "read" rights to the users folder so that
        //   the public profile of all users is visible to all of the other users
        ConfigurationParameters userConfig = securityProvider.getConfiguration(UserConfiguration.class).getParameters();
        @NotNull
        String usersPath = userConfig.getConfigValue(UserConstants.PARAM_USER_PATH, (String)null);
        JackrabbitAccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList policy = AccessControlUtils.getAccessControlList(acMgr, usersPath);
        policy.addEntry(EveryonePrincipal.getInstance(), 
                getPrivileges(new String[] {
                        PrivilegeConstants.JCR_READ
                    },
                acMgr),
                true);
        acMgr.setPolicy(usersPath, policy);

        // create some test users and groups for the tests
        User user1 = getTestUser();
        User user2 = getTestUser2();
        Group group1 = getTestGroup();
        // only members of a group can see the group so add
        // our test user to the group
        group1.addMember(user1);
        root.commit();

        // start sessions for the test users so we can validate what is
        //  visible and accessible in  the context of those users
        user1Session = login(new SimpleCredentials(user1.getID(), user1.getID().toCharArray()));
        user2Session = login(new SimpleCredentials(user2.getID(), user2.getID().toCharArray()));
    }

    /**
     * Cleanup after each test
     */
    @After
    public void after() throws Exception {
        if (user1Session != null) {
            user1Session.close();
        }
        if (user2Session != null) {
            user2Session.close();
        }
        removeTestUser2();
        removeTestGroup();

        // delegate the rest
        super.after();
    }

    protected User getTestUser2() throws Exception {
        if (testUser2 == null) {
            String uid = "testUser" + UUID.randomUUID();
            testUser2 = getUserManager(root).createUser(uid, uid);
            root.commit();
        }
        return testUser2;
    }

    protected void removeTestUser2() throws Exception {
        if (testUser2 != null) {
            testUser2.remove();
            root.commit();
            testUser2 = null;
        }
    }

    protected Group getTestGroup() throws Exception {
        if (testGroup == null) {
            String uid = "testGroup" + UUID.randomUUID();
            testGroup = getUserManager(root).createGroup(uid);
            root.commit();
        }
        return testGroup;
    }

    protected void removeTestGroup() throws Exception {
        if (testGroup != null) {
            testGroup.remove();
            root.commit();
            testGroup = null;
        }
    }

    /**
     * Creates the AuthorizableProperties object for the specified context
     * 
     * @param root the root for the user doing the lookup
     * @param principalName the name to get
     * @return the created object or null if the authorizable could not be found
     */
    protected @Nullable AuthorizablePropertiesImpl getAuthorizableProperties(Root root,
            String principalName) throws RepositoryException {
        AuthorizablePropertiesImpl properties = null;
        // lookup the users and group in the context of the test user sessions
        @Nullable
        Authorizable authorizable = getUserManager(root).getAuthorizable(principalName);

        // prepare the properties objects to be validated in the tests
        if (authorizable != null) {
            properties = new AuthorizablePropertiesImpl((AuthorizableImpl) authorizable, getPartialValueFactory());
        }
        return properties;
    }

    /**
     * Common successful getPropertyNames code to avoid duplicated code by multiple tests
     * 
     * @param session the session context to do the work
     * @param authorizable the user or group to read
     * @param propName the property name to set
     * @param newValue the property value to set
     */
    private void readPropertyNames(ContentSession session, Authorizable authorizable, String relPath, String expectedPropName) throws Exception {
        Authorizable user = getUserManager(session.getLatestRoot()).getAuthorizable(authorizable.getID());
        @NotNull
        Iterator<String> names = relPath == null ? user.getPropertyNames() : user.getPropertyNames(relPath);
        assertNotNull(names);
        boolean foundProp = false;
        while (names.hasNext()) {
            String name = names.next();
            if (expectedPropName.equals(name)) {
                foundProp = true;
            }
        }
        assertTrue("Did not find the expected property name", foundProp);
    }

    /**
     * Common successful getProperty code to avoid duplicated code by multiple tests
     * 
     * @param session the session context to do the work
     * @param authorizable the user or group to read
     * @param propName the property name to set
     * @param newValue the property value to set
     */
    private void readProperty(ContentSession session, Authorizable authorizable, String propName, String expectedValue) throws Exception {
        assertNotNull(authorizable);
        @NotNull
        Root root = session.getLatestRoot();
        AuthorizablePropertiesImpl authorizableProperties = getAuthorizableProperties(root, authorizable.getID());
        assertNotNull(authorizableProperties);
        assertTrue(authorizableProperties.hasProperty(propName));
        Value[] property = authorizableProperties.getProperty(propName);
        assertNotNull(property);
        assertEquals(1, property.length);
        assertEquals(expectedValue, property[0].getString());
    }

    /**
     * Common successful setProperty code to avoid duplicated code by multiple tests
     * 
     * @param session the session context to do the work
     * @param authorizable the user or group to alter
     * @param propName the property name to set
     * @param newValue the property value to set
     */
    private void alterProperty(ContentSession session, Authorizable authorizable, String propName, String newValue) throws Exception {
        assertNotNull(authorizable);
        @NotNull
        Root root = session.getLatestRoot();
        AuthorizablePropertiesImpl authorizableProperties = getAuthorizableProperties(root, authorizable.getID());
        assertNotNull(authorizableProperties);
        authorizableProperties.setProperty(propName, getValueFactory().createValue(newValue));
        root.commit();

        // and now re-read to verify it was persisted
        authorizableProperties = getAuthorizableProperties(session.getLatestRoot(), authorizable.getID());
        assertTrue(authorizableProperties.hasProperty(propName));
        Value[] property = authorizableProperties.getProperty(propName);
        assertNotNull(property);
        assertEquals(1, property.length);
        assertEquals(newValue, property[0].getString());
    }

    /**
     * Common failing setProperty code to avoid duplicated code by multiple tests
     * 
     * @param session the session context to do the work
     * @param authorizable the user or group to alter
     * @param propName the property name to set
     * @param newValue the property value to set
     */
    private void failToSetPropertyWithAccessDenied(ContentSession session, Authorizable authorizable, String propName, String newValue) throws Exception {
        assertNotNull(authorizable);
        @NotNull
        Root root = session.getLatestRoot();
        AuthorizablePropertiesImpl authorizableProperties = getAuthorizableProperties(root, authorizable.getID());
        assertNotNull(authorizableProperties);
        authorizableProperties.setProperty(propName, getValueFactory().createValue(newValue));
        // this should throw a AccessDeniedException since user doesn't have rights
        CommitFailedException error = assertThrows(CommitFailedException.class, () -> root.commit());
        assertTrue(error.asRepositoryException() instanceof AccessDeniedException);
    }

    /**
     * Common failing setProperty code to avoid duplicated code by multiple tests
     * 
     * @param session the session context to do the work
     * @param authorizable the user or group to alter
     * @param propName the property name to set
     * @param newValue the property value to set
     */
    private void failToSetPropertyWithConstraintViolation(ContentSession session, Authorizable authorizable, String propName, String newValue) throws Exception {
        assertNotNull(authorizable);
        @NotNull
        Root root = session.getLatestRoot();
        AuthorizablePropertiesImpl authorizableProperties = getAuthorizableProperties(root, authorizable.getID());
        assertNotNull(authorizableProperties);
        authorizableProperties.setProperty(propName, getValueFactory().createValue(newValue));
        // this should throw a AccessDeniedException since user doesn't have rights
        CommitFailedException error = assertThrows(CommitFailedException.class, () -> root.commit());
        assertTrue(error.asRepositoryException() instanceof ConstraintViolationException);
    }

    /**
     * Common failing removeProperty code to avoid duplicated code by multiple tests
     * 
     * @param session the session context to do the work
     * @param authorizable the user or group to alter
     * @param propName the property name to remove
     */
    private void failToRemovePropertyWithAccessDenied(ContentSession session, Authorizable authorizable, String propName) throws Exception {
        assertNotNull(authorizable);
        @NotNull
        Root root = session.getLatestRoot();
        AuthorizablePropertiesImpl authorizableProperties = getAuthorizableProperties(root, authorizable.getID());
        assertNotNull(authorizableProperties);
        authorizableProperties.removeProperty(propName);
        // this should throw a AccessDeniedException since user doesn't have rights
        CommitFailedException error = assertThrows(CommitFailedException.class, () -> root.commit());
        assertTrue(error.asRepositoryException() instanceof AccessDeniedException);
    }

    /**
     * Verify that the configured mixin defined property names are included in the 
     * property names
     */
    @Test
    public void testGetUser1PublicPropertyNamesByUser2() throws Exception {
        readPropertyNames(user2Session, getTestUser(), null, PROP_DISPLAYNAME);
    }

    /**
     * Verify that the configured primary type defined subnode property names are included in the 
     * property names
     */
    @Test
    public void testGetUser1PrivatePropertyNamesByUser1() throws Exception {
        readPropertyNames(user1Session, getTestUser(), "private", "email");
    }

    /**
     * Verify that the configured mixin defined property names are included in the 
     * property names
     */
    @Test
    public void testGetGroupPublicPropertyNamesByUser1() throws Exception {
        readPropertyNames(user1Session, getTestGroup(), null, PROP_DISPLAYNAME);
    }

    /**
     * Verify that the mixin defined property can be read by self
     */
    @Test
    public void testGetUser1PublicPropertyByUser1() throws Exception {
        readProperty(user1Session, getTestUser(), PROP_DISPLAYNAME, getTestUser().getID());
    }

    /**
     * Verify that the mixin defined property can be read by a different user
     */
    @Test
    public void testGetUser1PublicPropertyByUser2() throws Exception {
        readProperty(user2Session, getTestUser(), PROP_DISPLAYNAME, getTestUser().getID());
    }

    /**
     * Verify that the mixin defined property can be read by a user who is a 
     * member of the group
     */
    @Test
    public void testGetGroupPublicPropertyByUser1() throws Exception {
        readProperty(user1Session, getTestGroup(), PROP_DISPLAYNAME, getTestGroup().getID());
    }

    /**
     * Verify that the group is not visible to a user who is not a
     * member of the group
     */
    @Test
    public void testFailToGetGroupPublicPropertyByUser2() throws Exception {
        AuthorizablePropertiesImpl authorizableProperties = getAuthorizableProperties(user2Session.getLatestRoot(), getTestGroup().getID());
        assertNull(authorizableProperties);
    }

    /**
     * Verify that the primary type defined subnode property can be read by a self
     */
    @Test
    public void testGetUser1PrivatePropertyByUser1() throws Exception {
        readProperty(user1Session, getTestUser(), PROP_PRIVATE_EMAIL, INITIAL_EMAIL_VALUE);
    }

    /**
     * Verify that the primary type defined subnode property can not be read by 
     * another user
     */
    @Test
    public void testFailToGetUser1PrivatePropertyByUser2() throws Exception {
        AuthorizablePropertiesImpl authorizableProperties = getAuthorizableProperties(user2Session.getLatestRoot(), getTestUser().getID());
        assertNotNull(authorizableProperties);
        assertFalse(authorizableProperties.hasProperty(PROP_PRIVATE_EMAIL));
    }

    /**
     * Verify that the mixin defined property can be altered by self
     */
    @Test
    public void testAlterUser1PublicPropertyByUser1() throws Exception {
        alterProperty(user1Session, getTestUser(), PROP_DISPLAYNAME, "changed");
    }

    /**
     * Verify that the mixin type defined property can not be altered by another user
     */
    @Test
    public void testFailToAlterUser1PublicPropertyByUser2() throws Exception {
        failToSetPropertyWithAccessDenied(user2Session, getTestUser(), PROP_DISPLAYNAME, "changed");
    }

    /**
     * Verify that the mixin defined property can not be altered by a member
     * of the group
     */
    @Test
    public void testFailToAlterGroupPublicPropertyByUser1() throws Exception {
        failToSetPropertyWithAccessDenied(user1Session, getTestGroup(), PROP_DISPLAYNAME, "changed");
    }

    /**
     * Verify that the mixin defined property can not be altered by admin
     */
    @Test
    public void testAlterGroupPublicPropertyByAdmin() throws Exception {
        alterProperty(adminSession, getTestGroup(), PROP_DISPLAYNAME, "changed");
    }

    /**
     * Verify that the primary type defined subnode property can be altered by self
     */
    @Test
    public void testAlterUser1PrivatePropertyByUser1() throws Exception {
        alterProperty(user1Session, getTestUser(), PROP_PRIVATE_EMAIL, "changed@example.com");
    }

    /**
     * Verify that the primary type defined subnode property can not be altered 
     * by another user
     */
    @Test
    public void testFailToAlterUser1PrivatePropertyByUser2() throws Exception {
        failToSetPropertyWithAccessDenied(user2Session, getTestUser(), PROP_PRIVATE_EMAIL, "changed@example.com");
    }

    /**
     * Verify that the mixin type defined property can not be altered by self when
     * the value violates the property definition value constraint
     */
    @Test
    public void testFailToAlterUser1PublicPropertyByUser1WithValueConstraintViolation() throws Exception {
        failToSetPropertyWithConstraintViolation(user1Session, getTestUser(), PROP_DISPLAYNAME, testText(101));
    }

    /**
     * Verify that the primary type defined property can not be altered by self when
     * the value violates the property definition value constraint
     */
    @Test
    public void testFailToAlterUser1PrivatePropertyByUser1WithValueConstraintViolation() throws Exception {
        failToSetPropertyWithConstraintViolation(user1Session, getTestUser(), PROP_PRIVATE_EMAIL, "not_an_email_address");
    }

    /**
     * Verify that a new undefined property can not be added by self
     */
    @Test
    public void testFailToAddUser1PublicPropertyByUser1() throws Exception {
        failToSetPropertyWithAccessDenied(user1Session, getTestUser(), "not_already_existing", "hello");
    }

    /**
     * Verify that a new undefined property can not be added by a non-admin user
     */
    @Test
    public void testFailToAddGroupPublicPropertyByUser1() throws Exception {
        failToSetPropertyWithAccessDenied(user1Session, getTestGroup(), "not_already_existing", "hello");
    }

    /**
     * Verify that a new undefined property can be added by the admin user
     */
    @Test
    public void testAddGroupPublicPropertyByAdmin() throws Exception {
        Group group = getTestGroup();
        assertNotNull(group);
        @NotNull
        Root adminRoot = adminSession.getLatestRoot();
        AuthorizablePropertiesImpl authorizableProperties = getAuthorizableProperties(adminRoot, group.getID());
        assertNotNull(authorizableProperties);
        authorizableProperties.setProperty("not_already_existing", getValueFactory().createValue("hello"));
        // this should throw succeed since admin has rights to add new properties
        adminRoot.commit();

        // and now re-read to verify it was persisted
        authorizableProperties = getAuthorizableProperties(adminSession.getLatestRoot(), group.getID());
        assertTrue(authorizableProperties.hasProperty("not_already_existing"));
        Value[] property = authorizableProperties.getProperty("not_already_existing");
        assertNotNull(property);
        assertEquals(1, property.length);
        assertEquals("hello", property[0].getString());
    }

    /**
     * Verify that a new undefined subnode property can not be added by self as it
     * is not allowed to add children to the user home node
     */
    @Test
    public void testFailToAddUser1NestedPropertyByUser1() throws Exception {
        failToSetPropertyWithAccessDenied(user1Session, getTestUser2(), "not_already_existing/prop1", "hello");
    }

    /**
     * Verify that a new defined subnode property can not be added by self as it
     * is not allowed to add properties
     */
    @Test
    public void testFailToAddUser1PrivatePropertyByUser1() throws Exception {
        failToSetPropertyWithAccessDenied(user1Session, getTestUser(), "private/prop1", "hello");
    }

    /**
     * Verify that a new undefined subnode property can not be added by self as it
     * is not allowed by the subtype property type node definition
     */
    @Test
    public void testFailToAddUndefinedUser1PrivatePropertyByUser1() throws Exception {
        User user = getTestUser();
        assertNotNull(user);
        @NotNull
        Root user1Root = user1Session.getLatestRoot();
        AuthorizablePropertiesImpl authorizableProperties = getAuthorizableProperties(user1Root, user.getID());
        assertNotNull(authorizableProperties);
        // this should throw a ConstraintViolationException since "not_existing" is not a defined property in the node type
        assertThrows(ConstraintViolationException.class, () -> authorizableProperties.setProperty("private/not_existing", getValueFactory().createValue("hello")));
    }

    /**
     * Verify that an existing mixin defined property can not be removed by self as it
     * is not allowed to remove properties
     */
    @Test
    public void testFailToRemoveUser1PublicPropertyByUser1() throws Exception {
        failToRemovePropertyWithAccessDenied(user1Session, getTestUser(), PROP_DISPLAYNAME);
    }

    /**
     * Verify that an existing primary type defined subnode property can not be removed by self as it
     * is not allowed to remove properties
     */
    @Test
    public void testFailToRemoveUser1PrivatePropertyByUser1() throws Exception {
        failToRemovePropertyWithAccessDenied(user1Session, getTestUser(), PROP_PRIVATE_EMAIL);
    }


    /**
     * A custom AuthorizableAction to assign a custom mixin to the user/group
     * home folder and pre-create properties that the end user should be 
     * allowed to alter.
     */
    private static class PublicProfileAction extends AbstractAuthorizableAction {
        private static final Logger log = LoggerFactory.getLogger(PublicProfileAction.class);

        private SecurityProvider securityProvider;

        @Override
        public void init(SecurityProvider securityProvider, ConfigurationParameters config) {
            this.securityProvider = securityProvider;
        }

        /* (non-Javadoc)
         * @see org.apache.jackrabbit.oak.spi.security.user.action.AbstractAuthorizableAction#onCreate(org.apache.jackrabbit.api.security.user.User, java.lang.String, org.apache.jackrabbit.oak.api.Root, org.apache.jackrabbit.oak.namepath.NamePathMapper)
         */
        @Override
        public void onCreate(User user, String password, Root root, NamePathMapper namePathMapper)
                throws RepositoryException {
            if (securityProvider == null) {
                throw new IllegalStateException("Not initialized");
            }

            log.debug("adding user: {}", user.getID());
            String userId = user.getID();

            ConfigurationParameters userConfig = securityProvider.getConfiguration(UserConfiguration.class).getParameters();
            if (UserUtil.getAdminId(userConfig).equals(userId) || UserUtil.getAnonymousId(userConfig).equals(userId)) {
                // skip these as they are created before we have registered custom the mixin type
                log.debug("System user: {}; omit public mixin type setup.", user.getID());
                return; 
            }

            Tree authorizableNode = root.getTree(user.getPath());
            if (authorizableNode.exists()) {
                // declare the mixin
                TreeUtil.addMixin(authorizableNode, "oak9765:userPublic", root.getTree(NODE_TYPES_PATH), userId);
                //pre-add properties as the user will only have rights to alterProperty and not to add a property
                PropertyState property = authorizableNode.getProperty(PROP_DISPLAYNAME);
                if (property == null) {
                    authorizableNode.setProperty(PROP_DISPLAYNAME, userId, Type.STRING);
                }
            }
        }

        @Override
        public void onCreate(@NotNull Group group, @NotNull Root root, @NotNull NamePathMapper namePathMapper)
                throws RepositoryException {
            log.debug("adding group: {}", group.getID());
            String groupId = group.getID();

            Tree authorizableNode = root.getTree(group.getPath());
            if (authorizableNode.exists()) {
                // declare the mixin
                TreeUtil.addMixin(authorizableNode, "oak9765:groupPublic", root.getTree(NODE_TYPES_PATH), groupId);
                //pre-add properties as the group will only have rights to alterProperty and not to add a property
                PropertyState property = authorizableNode.getProperty(PROP_DISPLAYNAME);
                if (property == null) {
                    authorizableNode.setProperty(PROP_DISPLAYNAME, groupId, Type.STRING);
                }
            }
        }

    }

    /**
     * A custom AuthorizableAction to create a "private" subnode under the 
     * user home folder and pre-create properties that the end user should be 
     * allowed to alter.
     * 
     * Also change the ACL so that the "everyone" principal is denied read 
     * privilege for this "private" subnode.
     */
    private static class PrivateProfileAction extends AbstractAuthorizableAction {
        private static final Logger log = LoggerFactory.getLogger(PrivateProfileAction.class);

        private SecurityProvider securityProvider;

        public PrivateProfileAction() {
            super();
        }

        @Override
        public void init(SecurityProvider securityProvider, ConfigurationParameters config) {
            this.securityProvider = securityProvider;
        }

        /* (non-Javadoc)
         * @see org.apache.jackrabbit.oak.spi.security.user.action.AbstractAuthorizableAction#onCreate(org.apache.jackrabbit.api.security.user.User, java.lang.String, org.apache.jackrabbit.oak.api.Root, org.apache.jackrabbit.oak.namepath.NamePathMapper)
         */
        @Override
        public void onCreate(User user, String password, Root root, NamePathMapper namePathMapper)
                throws RepositoryException {
            if (securityProvider == null) {
                throw new IllegalStateException("Not initialized");
            }

            log.debug("adding user: {}", user.getID());
            ConfigurationParameters userConfig = securityProvider.getConfiguration(UserConfiguration.class).getParameters();
            String userId = user.getID();
            if (UserUtil.getAdminId(userConfig).equals(userId) || UserUtil.getAnonymousId(userConfig).equals(userId)) {
                log.debug("System user: {}; omit private child setup.", user.getID());
                return;
            }

            Tree authorizableNode = root.getTree(user.getPath());
            if (authorizableNode.exists()) {
                // pre-create the child node using our custom primary type
                Tree privateNode = TreeUtil.addChild(authorizableNode, "private", "oak9765:userPrivate");

                //pre-add properties as the user will only have rights to alterProperty and not to add a property
                PropertyState property = privateNode.getProperty(PROP_DISPLAYNAME);
                if (property == null) {
                    privateNode.setProperty("email", INITIAL_EMAIL_VALUE, Type.STRING);
                }

                //change the access control to deny read privilege to everyone except the user and admin
                String path = privateNode.getPath();

                AuthorizationConfiguration acConfig = securityProvider.getConfiguration(AuthorizationConfiguration.class);
                AccessControlManager acMgr = acConfig.getAccessControlManager(root, namePathMapper);
                JackrabbitAccessControlList policy = AccessControlUtils.getAccessControlList(acMgr, path);
                policy.addEntry(EveryonePrincipal.getInstance(),
                        getPrivileges(new String[] {
                                PrivilegeConstants.JCR_READ
                            },
                        acMgr),
                        false); // deny
                acMgr.setPolicy(path, policy);
            }
        }

    }

    /**
     * Utility to retrieve privileges for the specified privilege names.
     *
     * @param privNames The privilege names.
     * @param acMgr The access control manager.
     * @return Array of {@code Privilege}
     * @throws javax.jcr.RepositoryException If a privilege name cannot be
     * resolved to a valid privilege.
     */
    private static Privilege[] getPrivileges(String[] privNames,
                                             AccessControlManager acMgr) throws RepositoryException {
        if (privNames == null || privNames.length == 0) {
            return new Privilege[0];
        }
        Privilege[] privileges = new Privilege[privNames.length];
        for (int i = 0; i < privNames.length; i++) {
            privileges[i] = acMgr.privilegeFromName(privNames[i]);
        }
        return privileges;
    }

    protected static final char[] testChars = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ' '}; 
    /**
     * Utility to create a text string of a specified length
     * @param length the length of the text to produce
     * @return a string of the specified length
     */
    protected static String testText(int length) {
        StringBuilder builder = new StringBuilder();
        for (int i=0; i < length; i++) {
            int remainder = i % testChars.length;
            builder.append(testChars[remainder]);
        }
        return builder.toString();
    }

}
