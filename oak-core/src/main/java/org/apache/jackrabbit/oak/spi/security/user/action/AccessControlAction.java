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

import java.security.Principal;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code AccessControlAction} allows to setup permissions upon creation
 * of a new authorizable; namely the privileges the new authorizable should be
 * granted on it's own 'home directory' being represented by the new node
 * associated with that new authorizable.
 *
 * <p>The following to configuration parameters are available with this implementation:</p>
 * <ul>
 *    <li><strong>groupPrivilegeNames</strong>: the value is expected to be a
 *    comma separated list of privileges that will be granted to the new group on
 *    the group node</li>
 *    <li><strong>userPrivilegeNames</strong>: the value is expected to be a
 *    comma separated list of privileges that will be granted to the new user on
 *    the user node.</li>
 * </ul>
 * <p>Example configuration:
 * <pre>
 *    groupPrivilegeNames : ["jcr:read"]
 *    userPrivilegeNames  : ["jcr:read,rep:write"]
 * </pre>
 * <p>This configuration could for example lead to the following content
 * structure upon user or group creation. Note however that the resulting
 * structure depends on the actual access control management being in place:</p>
 *
 * <pre>
 *     UserManager umgr = ((JackrabbitSession) session).getUserManager();
 *     User user = umgr.createUser("testUser", "t");
 *
 *     + t                           rep:AuthorizableFolder
 *       + te                        rep:AuthorizableFolder
 *         + testUser                rep:User, mix:AccessControllable
 *           + rep:policy            rep:ACL
 *             + allow               rep:GrantACE
 *               - rep:principalName = "testUser"
 *               - rep:privileges    = ["jcr:read","rep:write"]
 *           - rep:password
 *           - rep:principalName     = "testUser"
 * </pre>
 *
 * <pre>
 *     UserManager umgr = ((JackrabbitSession) session).getUserManager();
 *     Group group = umgr.createGroup("testGroup");
 *
 *     + t                           rep:AuthorizableFolder
 *       + te                        rep:AuthorizableFolder
 *         + testGroup               rep:Group, mix:AccessControllable
 *           + rep:policy            rep:ACL
 *             + allow               rep:GrantACE
 *               - rep:principalName = "testGroup"
 *               - rep:privileges    = ["jcr:read"]
 *           - rep:principalName     = "testGroup"
 * </pre>
 */
public class AccessControlAction extends AbstractAuthorizableAction {

    private static final Logger log = LoggerFactory.getLogger(AccessControlAction.class);

    public static final String USER_PRIVILEGE_NAMES = "userPrivilegeNames";
    public static final String GROUP_PRIVILEGE_NAMES = "groupPrivilegeNames";

    private SecurityProvider securityProvider;
    private String[] groupPrivilegeNames = new String[0];
    private String[] userPrivilegeNames = new String[0];
    private Set<String> administrativePrincipals = Collections.emptySet();

    //-------------------------------------------------< AuthorizableAction >---
    @Override
    public void init(SecurityProvider securityProvider, ConfigurationParameters config) {
        this.securityProvider = securityProvider;
        userPrivilegeNames = privilegeNames(config, USER_PRIVILEGE_NAMES);
        groupPrivilegeNames = privilegeNames(config, GROUP_PRIVILEGE_NAMES);

        administrativePrincipals = securityProvider.getConfiguration(AuthorizationConfiguration.class).getParameters().getConfigValue(PermissionConstants.PARAM_ADMINISTRATIVE_PRINCIPALS, Collections.EMPTY_SET);
    }

    @Override
    public void onCreate(@Nonnull Group group, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
        setAC(group, root, namePathMapper);
    }

    @Override
    public void onCreate(@Nonnull User user, @Nullable String password, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
        setAC(user, root, namePathMapper);
    }

    //------------------------------------------------------------< private >---
    /**
     * Gets the privilege names that be granted on the group's home directory.
     *
     * @param config the configuration parameters.
     * @param paramName the name of the configuration option
     * @return a valid string array containing the privilege names.
     */
    private static String[] privilegeNames(ConfigurationParameters config, String paramName) {
        String[] privilegeNames = config.getConfigValue(paramName, null, String[].class);
        if (privilegeNames != null && privilegeNames.length > 0) {
            return privilegeNames;
        } else {
            return new String[0];
        }
    }

    private void setAC(@Nonnull Authorizable authorizable, @Nonnull Root root,
                       @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
        if (securityProvider == null) {
            throw new IllegalStateException("Not initialized");
        }
        if (authorizable.isGroup()) {
            if (groupPrivilegeNames.length == 0) {
                log.debug("No privileges configured for groups; omit ac setup.");
                return;
            }
        } else {
            if (userPrivilegeNames.length == 0) {
                log.debug("No privileges configured for users; omit ac setup.");
                return;
            }
            if (isBuiltInUser(authorizable)) {
                log.debug("System user: " + authorizable.getID() + "; omit ac setup.");
                return;
            }
        }

        Principal principal = authorizable.getPrincipal();
        if (administrativePrincipals.contains(principal.getName())) {
            log.debug("Administrative principal: " + principal.getName() + "; omit ac setup.");
            return;
        }

        String path = authorizable.getPath();
        AuthorizationConfiguration acConfig = securityProvider.getConfiguration(AuthorizationConfiguration.class);
        AccessControlManager acMgr = acConfig.getAccessControlManager(root, namePathMapper);
        JackrabbitAccessControlList acl = null;
        for (AccessControlPolicyIterator it = acMgr.getApplicablePolicies(path); it.hasNext();) {
            AccessControlPolicy plc = it.nextAccessControlPolicy();
            if (plc instanceof JackrabbitAccessControlList) {
                acl = (JackrabbitAccessControlList) plc;
                break;
            }
        }

        if (acl == null) {
            log.warn("Cannot process AccessControlAction: no applicable ACL at " + path);
        } else {
            // setup acl according to configuration.
            boolean modified = false;
            String[] privNames = (authorizable.isGroup()) ? groupPrivilegeNames : userPrivilegeNames;
            modified = acl.addAccessControlEntry(principal, getPrivileges(privNames, acMgr));
            if (modified) {
                acMgr.setPolicy(path, acl);
            }
        }
    }

    private boolean isBuiltInUser(@Nonnull Authorizable authorizable) throws RepositoryException {
        ConfigurationParameters userConfig = securityProvider.getConfiguration(UserConfiguration.class).getParameters();
        String userId = authorizable.getID();
        return UserUtil.getAdminId(userConfig).equals(userId) || UserUtil.getAnonymousId(userConfig).equals(userId);
    }

    /**
     * Retrieve privileges for the specified privilege names.
     *
     * @param privNames The privilege names.
     * @param acMgr The access control manager.
     * @return Array of {@code Privilege}
     * @throws javax.jcr.RepositoryException If a privilege name cannot be
     * resolved to a valid privilege.
     */
    private static Privilege[] getPrivileges(@Nullable String[] privNames,
                                             @Nonnull AccessControlManager acMgr) throws RepositoryException {
        if (privNames == null || privNames.length == 0) {
            return new Privilege[0];
        }
        Privilege[] privileges = new Privilege[privNames.length];
        for (int i = 0; i < privNames.length; i++) {
            privileges[i] = acMgr.privilegeFromName(privNames[i]);
        }
        return privileges;
    }
}
