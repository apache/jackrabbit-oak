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

import java.util.ArrayList;
import java.util.List;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code AccessControlAction} allows to setup permissions upon creation
 * of a new authorizable; namely the privileges the new authorizable should be
 * granted on it's own 'home directory' being represented by the new node
 * associated with that new authorizable.
 *
 * <p>The following to configuration parameters are available with this implementation:
 * <ul>
 *    <li><strong>groupPrivilegeNames</strong>: the value is expected to be a
 *    comma separated list of privileges that will be granted to the new group on
 *    the group node</li>
 *    <li><strong>userPrivilegeNames</strong>: the value is expected to be a
 *    comma separated list of privileges that will be granted to the new user on
 *    the user node.</li>
 * </ul>
 * </p>
 * <p>Example configuration:
 * <pre>
 *    groupPrivilegeNames : "jcr:read"
 *    userPrivilegeNames  : "jcr:read, rep:write"
 * </pre>
 * </p>
 * <p>This configuration could for example lead to the following content
 * structure upon user or group creation. Note however that the resulting
 * structure depends on the actual access control management being in place:
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
 * </p>
 */
public class AccessControlAction extends AbstractAuthorizableAction {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(AccessControlAction.class);

    private String[] groupPrivilegeNames = new String[0];
    private String[] userPrivilegeNames = new String[0];

    //-------------------------------------------------< AuthorizableAction >---
    @Override
    public void onCreate(Group group, Root root) throws RepositoryException {
        setAC(group, root);
    }

    @Override
    public void onCreate(User user, String password, Root root) throws RepositoryException {
        setAC(user, root);
    }

    //------------------------------------------------------< Configuration >---
    /**
     * Sets the privileges a new group will be granted on the group's home directory.
     *
     * @param privilegeNames A comma separated list of privilege names.
     */
    public void setGroupPrivilegeNames(String privilegeNames) {
        if (privilegeNames != null && privilegeNames.length() > 0) {
            groupPrivilegeNames = split(privilegeNames);
        }

    }

    /**
     * Sets the privileges a new user will be granted on the user's home directory.
     *
     * @param privilegeNames  A comma separated list of privilege names.
     */
    public void setUserPrivilegeNames(String privilegeNames) {
        if (privilegeNames != null && privilegeNames.length() > 0) {
            userPrivilegeNames = split(privilegeNames);
        }
    }

    //------------------------------------------------------------< private >---

    private void setAC(Authorizable authorizable, Root root) throws RepositoryException {
        // TODO: add implementation
        log.error("Not yet implemented");

//        Node aNode;
//        String path = authorizable.getPath();
//
//        JackrabbitAccessControlList acl = null;
//        AccessControlManager acMgr = session.getAccessControlManager();
//        for (AccessControlPolicyIterator it = acMgr.getApplicablePolicies(path); it.hasNext();) {
//            AccessControlPolicy plc = it.nextAccessControlPolicy();
//            if (plc instanceof JackrabbitAccessControlList) {
//                acl = (JackrabbitAccessControlList) plc;
//                break;
//            }
//        }
//
//        if (acl == null) {
//            log.warn("Cannot process AccessControlAction: no applicable ACL at " + path);
//        } else {
//            // setup acl according to configuration.
//            Principal principal = authorizable.getPrincipal();
//            boolean modified = false;
//            if (authorizable.isGroup()) {
//                // new authorizable is a Group
//                if (groupPrivilegeNames.length > 0) {
//                    modified = acl.addAccessControlEntry(principal, getPrivileges(groupPrivilegeNames, acMgr));
//                }
//            } else {
//                // new authorizable is a User
//                if (userPrivilegeNames.length > 0) {
//                    modified = acl.addAccessControlEntry(principal, getPrivileges(userPrivilegeNames, acMgr));
//                }
//            }
//            if (modified) {
//                acMgr.setPolicy(path, acl);
//            }
//        }
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
    private static Privilege[] getPrivileges(String[] privNames, AccessControlManager acMgr) throws RepositoryException {
        if (privNames == null || privNames.length == 0) {
            return new Privilege[0];
        }
        Privilege[] privileges = new Privilege[privNames.length];
        for (int i = 0; i < privNames.length; i++) {
            privileges[i] = acMgr.privilegeFromName(privNames[i]);
        }
        return privileges;
    }

    /**
     * Split the specified configuration parameter into privilege names.
     *
     * @param configParam The configuration parameter defining a comma separated
     * list of privilege names.
     * @return An array of privilege names.
     */
    private static String[] split(String configParam) {
        List<String> nameList = new ArrayList<String>();
        for (String pn : Text.explode(configParam, ',', false)) {
            String privName = pn.trim();
            if (privName.length()  > 0) {
                nameList.add(privName);
            }
        }
        return nameList.toArray(new String[nameList.size()]);
    }
}
