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

import java.io.UnsupportedEncodingException;
import java.util.UUID;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserManagerConfig;
import org.apache.jackrabbit.oak.spi.security.user.UserProvider;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User provider implementation creating the JCR nodes corresponding the a given
 * authorizable ID with the following behavior:
 * <ul>
 * <li>Users are created below /rep:security/rep:authorizables/rep:users or
 * the path configured in the {@link org.apache.jackrabbit.oak.spi.security.user.UserManagerConfig#PARAM_USER_PATH}
 * respectively.</li>
 * <li>Groups are created below /rep:security/rep:authorizables/rep:groups or
 * the path configured in the {@link org.apache.jackrabbit.oak.spi.security.user.UserManagerConfig#PARAM_GROUP_PATH}
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
 *     <li>{@link org.apache.jackrabbit.oak.spi.security.user.UserManagerConfig#PARAM_USER_PATH}: Underneath this structure
 *     all user nodes are created. Default value is
 *     "/rep:security/rep:authorizables/rep:users"</li>
 *     <li>{@link org.apache.jackrabbit.oak.spi.security.user.UserManagerConfig#PARAM_GROUP_PATH}: Underneath this structure
 *     all group nodes are created. Default value is
 *     "/rep:security/rep:authorizables/rep:groups"</li>
 *     <li>{@link org.apache.jackrabbit.oak.spi.security.user.UserManagerConfig#PARAM_DEFAULT_DEPTH}: A positive {@code integer}
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
 */
public class UserProviderImpl implements UserProvider {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(UserProviderImpl.class);

    private static final String DELIMITER = "/";
    private static final int DEFAULT_DEPTH = 2;

    private final Root root;
    private final CoreValueFactory valueFactory;
    private final NameMapper nameMapper;

    private final int defaultDepth;

    private final String groupPath;
    private final String userPath;

    public UserProviderImpl(Root root, CoreValueFactory valueFactory,
                            NameMapper nameMapper,
                            UserManagerConfig config) {
        this.root = root;
        this.valueFactory = valueFactory;
        this.nameMapper = nameMapper;

        defaultDepth = config.getConfigValue(UserManagerConfig.PARAM_DEFAULT_DEPTH, DEFAULT_DEPTH);

        groupPath = config.getConfigValue(UserManagerConfig.PARAM_GROUP_PATH, UserConstants.DEFAULT_GROUP_PATH);
        userPath = config.getConfigValue(UserManagerConfig.PARAM_USER_PATH, UserConstants.DEFAULT_USER_PATH);
    }

    @Override
    public String createUser(String userID, String intermediateJcrPath) throws RepositoryException {
        return createAuthorizableNode(userID, false, intermediateJcrPath);
    }

    @Override
    public String createGroup(String groupID, String intermediateJcrPath) throws RepositoryException {
        return createAuthorizableNode(groupID, true, intermediateJcrPath);
    }

    @Override
    public String getContentID(String authorizableId) throws RepositoryException {
        try {
            UUID uuid = UUID.nameUUIDFromBytes(authorizableId.toLowerCase().getBytes("UTF-8"));
            return uuid.toString();
        } catch (UnsupportedEncodingException e) {
            throw new RepositoryException("Unexpected error while creating authorizable node", e);
        }
    }

    private String createAuthorizableNode(String authorizableId, boolean isGroup, String intermediatePath) throws RepositoryException {
        String nodeName = Text.escapeIllegalJcrChars(authorizableId);
        NodeUtil folder = createFolderNodes(authorizableId, nodeName, isGroup, intermediatePath);

        String ntName = (isGroup) ? UserConstants.NT_REP_GROUP : UserConstants.NT_REP_USER;
        NodeUtil authorizableNode = folder.addChild(nodeName, ntName);

        String nodeID = getContentID(authorizableId);
        authorizableNode.setString(JcrConstants.JCR_UUID, nodeID);

        return authorizableNode.getTree().getPath();
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
            folder = new NodeUtil(valueFactory, nameMapper, root.getTree(""));
            for (String name : Text.explode(authRoot, '/', false)) {
                folder = folder.getOrAddChild(name, UserConstants.NT_REP_AUTHORIZABLE_FOLDER);
            }
        }  else {
            folder = new NodeUtil(valueFactory, nameMapper, authTree);
        }
        String folderPath = getFolderPath(authorizableId, intermediatePath);
        String[] segmts = Text.explode(folderPath, '/', false);
        for (String segment : segmts) {
            folder = folder.getOrAddChild(segment, UserConstants.NT_REP_AUTHORIZABLE_FOLDER);
            // TODO: remove check once UserValidator is active
            if (!folder.hasPrimaryNodeTypeName(UserConstants.NT_REP_AUTHORIZABLE_FOLDER)) {
                String msg = "Cannot create user/group: Intermediate folders must be of type rep:AuthorizableFolder.";
                throw new ConstraintViolationException(msg);
            }
        }

        // test for colliding folder child node.
        while (folder.hasChild(nodeName)) {
            NodeUtil colliding = folder.getChild(nodeName);
            // TODO: remove check once UserValidator is active
            if (colliding.hasPrimaryNodeTypeName(UserConstants.NT_REP_AUTHORIZABLE_FOLDER)) {
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