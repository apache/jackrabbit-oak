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

import java.security.Principal;
import java.text.ParseException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.query.Query;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.principal.TreeBasedPrincipal;
import org.apache.jackrabbit.oak.spi.security.user.Type;
import org.apache.jackrabbit.oak.spi.security.user.UserConfig;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserProvider;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User provider implementation and manager for group memberships with the
 * following characteristics:
 *
 * <h1>UserProvider</h1>
 *
 * <h2>User and Group Creation</h2>
 * This implementation creates the JCR nodes corresponding the a given
 * authorizable ID with the following behavior:
 * <ul>
 * <li>Users are created below /rep:security/rep:authorizables/rep:users or
 * the path configured in the {@link org.apache.jackrabbit.oak.spi.security.user.UserConfig#PARAM_USER_PATH}
 * respectively.</li>
 * <li>Groups are created below /rep:security/rep:authorizables/rep:groups or
 * the path configured in the {@link org.apache.jackrabbit.oak.spi.security.user.UserConfig#PARAM_GROUP_PATH}
 * respectively.</li>
 * <li>Below each category authorizables are created within a human readable
 * structure based on the defined intermediate path or some internal logic
 * with a depth defined by the {@code defaultDepth} config option.<br>
 * E.g. creating a user node for an ID 'aSmith' would result in the following
 * structure assuming defaultDepth == 2 is used:
 * <pre>
 * + rep:security            [rep:AuthorizableFolder]
 *   + rep:authorizables     [rep:AuthorizableFolder]
 *     + rep:users           [rep:AuthorizableFolder]
 *       + a                 [rep:AuthorizableFolder]
 *         + aS              [rep:AuthorizableFolder]
 * ->        + aSmith        [rep:User]
 * </pre>
 * </li>
 * <li>The node name is calculated from the specified authorizable ID
 * {@link org.apache.jackrabbit.util.Text#escapeIllegalJcrChars(String) escaping} any illegal JCR chars.</li>
 * <li>If no intermediate path is passed the names of the intermediate
 * folders are calculated from the leading chars of the escaped node name.</li>
 * <li>If the escaped node name is shorter than the {@code defaultDepth}
 * the last char is repeated.<br>
 * E.g. creating a user node for an ID 'a' would result in the following
 * structure assuming defaultDepth == 2 is used:
 * <pre>
 * + rep:security            [rep:AuthorizableFolder]
 *   + rep:authorizables     [rep:AuthorizableFolder]
 *     + rep:users           [rep:AuthorizableFolder]
 *       + a                 [rep:AuthorizableFolder]
 *         + aa              [rep:AuthorizableFolder]
 * ->        + a             [rep:User]
 * </pre></li>
 *
 * <h3>Conflicts</h3>
 *
 * <ul>
 *     <li>If the authorizable node to be created would collide with an existing
 *     folder the conflict is resolved by using the colling folder as target.</li>
 *     <li>The current implementation asserts that authorizable nodes are always
 *     created underneath an node of type {@code rep:AuthorizableFolder}. If this
 *     condition is violated a {@code ConstraintViolationException} is thrown.</li>
 *     <li>If the specified intermediate path results in an authorizable node
 *     being located outside of the configured content structure a
 *     {@code ConstraintViolationException} is thrown.</li>
 * </ul>
 *
 * <h3>Configuration Options</h3>
 * <ul>
 *     <li>{@link org.apache.jackrabbit.oak.spi.security.user.UserConfig#PARAM_USER_PATH}: Underneath this structure
 *     all user nodes are created. Default value is
 *     "/rep:security/rep:authorizables/rep:users"</li>
 *     <li>{@link org.apache.jackrabbit.oak.spi.security.user.UserConfig#PARAM_GROUP_PATH}: Underneath this structure
 *     all group nodes are created. Default value is
 *     "/rep:security/rep:authorizables/rep:groups"</li>
 *     <li>{@link org.apache.jackrabbit.oak.spi.security.user.UserConfig#PARAM_DEFAULT_DEPTH}: A positive {@code integer}
 *     greater than zero defining the depth of the default structure that is
 *     always created. Default value: 2</li>
 * </ul>
 *
 * <h3>Compatibility with Jackrabbit 2.x</h3>
 *
 * Due to the fact that this JCR implementation is expected to deal with huge amount
 * of child nodes the following configuration options are no longer supported:
 * <ul>
 *     <li>autoExpandTree</li>
 *     <li>autoExpandSize</li>
 * </ul>
 *
 * <h2>User and Group Access</h2>
 * <h3>By ID</h3>
 * TODO
 * <h3>By Path</h3>
 * TODO
 * <h3>By Principal Name</h3>
 * TODO
 *
 * <h1>MembershipProvider</h1>
 *
 * TODO
 */
class UserProviderImpl extends AuthorizableBaseProvider implements UserProvider {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(UserProviderImpl.class);

    private static final String DELIMITER = "/";

    private final int defaultDepth;
    private final String adminId;

    private final String groupPath;
    private final String userPath;

    UserProviderImpl(ContentSession contentSession, Root root, UserConfig config) {
        super(contentSession, root, config);

        defaultDepth = config.getConfigValue(UserConfig.PARAM_DEFAULT_DEPTH, DEFAULT_DEPTH);
        adminId = config.getAdminId();

        groupPath = config.getConfigValue(UserConfig.PARAM_GROUP_PATH, DEFAULT_GROUP_PATH);
        userPath = config.getConfigValue(UserConfig.PARAM_USER_PATH, DEFAULT_USER_PATH);
    }

    //-------------------------------------------------------< UserProvider >---
    @Override
    public Tree createUser(String userID, String intermediateJcrPath) throws RepositoryException {
        return createAuthorizableNode(userID, false, intermediateJcrPath);
    }

    @Override
    public Tree createGroup(String groupID, String intermediateJcrPath) throws RepositoryException {
        return createAuthorizableNode(groupID, true, intermediateJcrPath);
    }

    @Override
    public Tree getAuthorizable(String authorizableId) {
        return getByID(authorizableId, Type.AUTHORIZABLE);
    }

    @Override
    public Tree getAuthorizable(String authorizableId, Type authorizableType) {
        return getByID(authorizableId, authorizableType);
    }

    @Override()
    public Tree getAuthorizableByPath(String authorizableOakPath) {
        return getByPath(authorizableOakPath);
    }

    @Override
    public Tree getAuthorizableByPrincipal(Principal principal) {
        Tree authorizableTree = null;
        if (principal instanceof TreeBasedPrincipal) {
            authorizableTree = root.getTree(((TreeBasedPrincipal) principal).getOakPath());
        } else {
            // NOTE: in contrast to JR2 the extra shortcut for ID==principalName
            // can be omitted as principals names are stored in user defined
            // index as well.
            try {
                CoreValue bindValue = valueFactory.createValue(principal.getName());
                Map<String, CoreValue> bindings = Collections.singletonMap("principalName", bindValue);
                String stmt = "SELECT * FROM [rep:Authorizable] WHERE [rep:principalName] = $principalName";
                Result result = queryEngine.executeQuery(stmt,
                        Query.JCR_SQL2, 1, 0,
                        Collections.singletonMap("principalName", bindValue),
                        new NamePathMapper.Default());

                Iterator rows = result.getRows().iterator();
                if (rows.hasNext()) {
                    String path = rows.next().toString();
                    authorizableTree = root.getTree(path);
                }
            } catch (ParseException ex) {
                log.error("query failed", ex);
            }
        }
        return authorizableTree;
    }

    @Override
    public String getAuthorizableId(Tree authorizableTree, Type authorizableType) {
        assert authorizableTree != null;
        if (isAuthorizableTree(authorizableTree, authorizableType)) {
            PropertyState idProp = authorizableTree.getProperty(UserConstants.REP_AUTHORIZABLE_ID);
            if (idProp != null) {
                return idProp.getValue().getString();
            } else {
                return Text.unescapeIllegalJcrChars(authorizableTree.getName());
            }
        }
        return null;
    }

    @Override
    public boolean isAdminUser(Tree userTree) {
        assert userTree != null;
        return adminId.equals(getAuthorizableId(userTree, Type.USER));
    }

    @Override
    public void setProtectedProperty(Tree authorizableTree, String propertyName, String value, int propertyType) {
        assert authorizableTree != null;

        if (value == null) {
            authorizableTree.removeProperty(propertyName);
        } else {
            CoreValue cv = valueFactory.createValue(value, propertyType);
            authorizableTree.setProperty(propertyName, cv);
        }
    }

    @Override
    public void setProtectedProperty(Tree authorizableTree, String propertyName, String[] values, int propertyType) {
        assert authorizableTree != null;

        if (values == null) {
            authorizableTree.removeProperty(propertyName);
        } else {
            NodeUtil node = new NodeUtil(authorizableTree, valueFactory);
            node.setValues(propertyName, values, propertyType);
        }
    }

    //------------------------------------------------------------< private >---

    private Tree createAuthorizableNode(String authorizableId, boolean isGroup, String intermediatePath) throws RepositoryException {
        String nodeName = Text.escapeIllegalJcrChars(authorizableId);
        NodeUtil folder = createFolderNodes(authorizableId, nodeName, isGroup, intermediatePath);

        String ntName = (isGroup) ? NT_REP_GROUP : NT_REP_USER;
        NodeUtil authorizableNode = folder.addChild(nodeName, ntName);

        String nodeID = getContentID(authorizableId);
        authorizableNode.setString(REP_AUTHORIZABLE_ID, authorizableId);
        authorizableNode.setString(JcrConstants.JCR_UUID, nodeID);

        return authorizableNode.getTree();
    }

    /**
     * Create folder structure for the authorizable to be created. The structure
     * consists of a tree of rep:AuthorizableFolder node(s) starting at the
     * configured user or group path. Note that Authorizable nodes are never
     * nested.
     *
     * @param authorizableId
     * @param nodeName
     * @param isGroup
     * @param intermediatePath
     * @return The folder node.
     * @throws RepositoryException If an error occurs
     */
    private NodeUtil createFolderNodes(String authorizableId, String nodeName,
                                   boolean isGroup, String intermediatePath) throws RepositoryException {
        String authRoot = (isGroup) ? groupPath : userPath;
        NodeUtil folder;
        Tree authTree = root.getTree(authRoot);
        if (authTree == null) {
            folder = new NodeUtil(root.getTree("/"), valueFactory);
            for (String name : Text.explode(authRoot, '/', false)) {
                folder = folder.getOrAddChild(name, NT_REP_AUTHORIZABLE_FOLDER);
            }
        }  else {
            folder = new NodeUtil(authTree, valueFactory);
        }
        String folderPath = getFolderPath(authorizableId, intermediatePath);
        String[] segmts = Text.explode(folderPath, '/', false);
        for (String segment : segmts) {
            folder = folder.getOrAddChild(segment, NT_REP_AUTHORIZABLE_FOLDER);
            // TODO: remove check once UserValidator is active
            if (!folder.hasPrimaryNodeTypeName(NT_REP_AUTHORIZABLE_FOLDER)) {
                String msg = "Cannot create user/group: Intermediate folders must be of type rep:AuthorizableFolder.";
                throw new ConstraintViolationException(msg);
            }
        }

        // test for colliding folder child node.
        while (folder.hasChild(nodeName)) {
            NodeUtil colliding = folder.getChild(nodeName);
            // TODO: remove check once UserValidator is active
            if (colliding.hasPrimaryNodeTypeName(NT_REP_AUTHORIZABLE_FOLDER)) {
                log.debug("Existing folder node collides with user/group to be created. Expanding path by: " + colliding.getName());
                folder = colliding;
            } else {
                String msg = "Failed to create authorizable with id '" + authorizableId + "' : " +
                        "Detected conflicting node of unexpected node type '" + colliding.getString(JcrConstants.JCR_PRIMARYTYPE, null) + "'.";
                log.error(msg);
                throw new ConstraintViolationException(msg);
            }
        }

        // TODO: remove check once UserValidator is active
        if (!Text.isDescendantOrEqual(authRoot, folder.getTree().getPath())) {
            throw new ConstraintViolationException("Attempt to create user/group outside of configured scope " + authRoot);
        }
        return folder;
    }

    private String getFolderPath(String authorizableId, String intermediatePath) {
        StringBuilder sb = new StringBuilder();
        if (intermediatePath != null && !intermediatePath.isEmpty()) {
            sb.append(intermediatePath);
        } else {
            int idLength = authorizableId.length();
            StringBuilder segment = new StringBuilder();
            for (int i = 0; i < defaultDepth; i++) {
                if (idLength > i) {
                    segment.append(authorizableId.charAt(i));
                } else {
                    // escapedID is too short -> append the last char again
                    segment.append(authorizableId.charAt(idLength-1));
                }
                sb.append(DELIMITER).append(Text.escapeIllegalJcrChars(segment.toString()));
            }
        }
        return sb.toString();
    }
}