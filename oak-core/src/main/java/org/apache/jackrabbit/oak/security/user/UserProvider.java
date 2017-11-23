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
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.query.Query;

import com.google.common.base.Strings;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

/**
 * User provider implementation and manager for group memberships with the
 * following characteristics:
 * <p>
 * <h1>UserProvider</h1>
 * <p>
 * <h2>User and Group Creation</h2>
 * This implementation creates the JCR nodes corresponding the a given
 * authorizable ID with the following behavior:
 * <ul>
 * <li>Users are created below /rep:security/rep:authorizables/rep:users or
 * the path configured in the {@link org.apache.jackrabbit.oak.spi.security.user.UserConstants#PARAM_USER_PATH}
 * respectively.</li>
 * <li>Groups are created below /rep:security/rep:authorizables/rep:groups or
 * the path configured in the {@link org.apache.jackrabbit.oak.spi.security.user.UserConstants#PARAM_GROUP_PATH}
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
 * <li>The node name is calculated from the specified authorizable ID according
 * to the logic provided by the configured {@link AuthorizableNodeName}
 * implementation. If no name generator is present in the configuration
 * the {@link AuthorizableNodeName#DEFAULT default} implementation is used. The
 * name of the configuration option is {@link UserConstants#PARAM_AUTHORIZABLE_NODE_NAME}</li>
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
 * <li>If the authorizable node to be created would collide with an existing
 * folder the conflict is resolved by using the colling folder as target.</li>
 * <li>The current implementation asserts that authorizable nodes are always
 * created underneath an node of type {@code rep:AuthorizableFolder}. If this
 * condition is violated a {@code ConstraintViolationException} is thrown.</li>
 * <li>If the specified intermediate path results in an authorizable node
 * being located outside of the configured content structure a
 * {@code ConstraintViolationException} is thrown.</li>
 * </ul>
 *
 * <h3>Configuration Options</h3>
 * <ul>
 * <li>{@link UserConstants#PARAM_USER_PATH}: Underneath this structure
 * all user nodes are created. Default value is
 * "/rep:security/rep:authorizables/rep:users"</li>
 * <li>{@link UserConstants#PARAM_GROUP_PATH}: Underneath this structure
 * all group nodes are created. Default value is
 * "/rep:security/rep:authorizables/rep:groups"</li>
 * <li>{@link UserConstants#PARAM_DEFAULT_DEPTH}: A positive {@code integer}
 * greater than zero defining the depth of the default structure that is
 * always created. Default value: 2</li>
 * <li>{@link UserConstants#PARAM_AUTHORIZABLE_NODE_NAME}: An implementation
 * of {@link AuthorizableNodeName} used to create a node name for a given
 * authorizableId. By {@link AuthorizableNodeName.Default default} the
 * ID itself is used as node name. (since OAK 1.0)</li>
 * </ul>
 *
 * <h3>Compatibility with Jackrabbit 2.x</h3>
 *
 * Due to the fact that this JCR implementation is expected to deal with huge amount
 * of child nodes the following configuration options are no longer supported:
 * <ul>
 * <li>autoExpandTree</li>
 * <li>autoExpandSize</li>
 * </ul>
 *
 * <h2>User and Group Access</h2>
 * <h3>By ID</h3>
 * Accessing authorizables by ID is achieved by calculating the ContentId
 * associated with that user/group and using {@link org.apache.jackrabbit.oak.api.QueryEngine}
 * to find the corresponding {@code Tree}. The result is validated to really
 * represent a user/group tree.
 *
 * <h3>By Path</h3>
 * Access by path consists of a simple lookup by path such as exposed by
 * {@link Root#getTree(String)}. The resulting tree is validated to really
 * represent a user/group tree.
 *
 * <h3>By Principal</h3>
 * If the principal instance passed to {@link #getAuthorizableByPrincipal(java.security.Principal)}
 * is a {@code TreeBasedPrincipal} the lookup is equivalent to
 * {@link #getAuthorizableByPath(String)}. Otherwise the user/group is search
 * for using {@link org.apache.jackrabbit.oak.api.QueryEngine} looking
 * for a property {@link UserConstants#REP_PRINCIPAL_NAME} that matches the
 * name of the specified principal.
 */
class UserProvider extends AuthorizableBaseProvider {

    private static final Logger log = LoggerFactory.getLogger(UserProvider.class);

    private static final String DELIMITER = "/";

    private final int defaultDepth;

    private final String groupPath;
    private final String userPath;

    UserProvider(@Nonnull Root root, @Nonnull ConfigurationParameters config) {
        super(root, config);

        defaultDepth = config.getConfigValue(PARAM_DEFAULT_DEPTH, DEFAULT_DEPTH);
        groupPath = config.getConfigValue(PARAM_GROUP_PATH, DEFAULT_GROUP_PATH);
        userPath = config.getConfigValue(PARAM_USER_PATH, DEFAULT_USER_PATH);
    }

    @Nonnull
    Tree createUser(@Nonnull String userID, @Nullable String intermediateJcrPath) throws RepositoryException {
        return createAuthorizableNode(userID, NT_REP_USER, intermediateJcrPath);
    }

    @Nonnull
    Tree createGroup(@Nonnull String groupID, @Nullable String intermediateJcrPath) throws RepositoryException {
        return createAuthorizableNode(groupID, NT_REP_GROUP, intermediateJcrPath);
    }

    @Nonnull
    Tree createSystemUser(@Nonnull String userID, @Nullable String intermediateJcrPath) throws RepositoryException {
        String relSysPath = config.getConfigValue(PARAM_SYSTEM_RELATIVE_PATH, DEFAULT_SYSTEM_RELATIVE_PATH);
        String relPath;
        if (intermediateJcrPath == null) {
            relPath = relSysPath;
        } else {
            if (!(intermediateJcrPath.startsWith(relSysPath) ||
                intermediateJcrPath.startsWith(userPath + '/' + relSysPath))) {
                throw new ConstraintViolationException("System users must be located in the 'system' subtree of the user root.");
            }
            relPath = intermediateJcrPath;
        }
        return createAuthorizableNode(userID, NT_REP_SYSTEM_USER, relPath);
    }

    @CheckForNull
    Tree getAuthorizable(@Nonnull String authorizableId) {
        return getByID(authorizableId, AuthorizableType.AUTHORIZABLE);
    }

    @CheckForNull
    Tree getAuthorizableByPath(@Nonnull String authorizableOakPath) {
        return getByPath(authorizableOakPath, AuthorizableType.AUTHORIZABLE);
    }

    @CheckForNull
    Tree getAuthorizableByPrincipal(@Nonnull Principal principal) {
        if (principal instanceof TreeBasedPrincipal) {
            return root.getTree(((TreeBasedPrincipal) principal).getOakPath());
        }

        // NOTE: in contrast to JR2 the extra shortcut for ID==principalName
        // can be omitted as principals names are stored in user defined
        // index as well.
        try {
            StringBuilder stmt = new StringBuilder();
            stmt.append("SELECT * FROM [").append(UserConstants.NT_REP_AUTHORIZABLE).append(']');
            stmt.append(" WHERE [").append(UserConstants.REP_PRINCIPAL_NAME).append("] = $principalName");
            stmt.append(QueryEngine.INTERNAL_SQL2_QUERY);

            Result result = root.getQueryEngine().executeQuery(stmt.toString(),
                    Query.JCR_SQL2, 1, 0,
                    Collections.singletonMap("principalName", PropertyValues.newString(principal.getName())),
                    NO_MAPPINGS);

            Iterator<? extends ResultRow> rows = result.getRows().iterator();
            if (rows.hasNext()) {
                String path = rows.next().getPath();
                return root.getTree(path);
            }
        } catch (ParseException ex) {
            log.error("Failed to retrieve authorizable by principal", ex);
        }

        return null;
    }

    //------------------------------------------------------------< private >---

    private Tree createAuthorizableNode(@Nonnull String authorizableId,
                                        @Nonnull String ntName,
                                        @Nullable String intermediatePath) throws RepositoryException {
        String nodeName = getNodeName(authorizableId);
        Tree folder = createFolderNodes(nodeName, NT_REP_GROUP.equals(ntName), intermediatePath);

        if (folder.hasChild(nodeName)) {
            // collision with another authorizable node or some other node type.
            int i = 1;
            String tmp = nodeName + i;
            while (folder.hasChild(tmp)) {
                tmp = nodeName + ++i;
            }
            nodeName = tmp;
        }

        Tree typeRoot = root.getTree(NODE_TYPES_PATH);
        String userId = Strings.nullToEmpty(root.getContentSession().getAuthInfo().getUserID());
        Tree authorizableNode = TreeUtil.addChild(folder, nodeName, ntName, typeRoot, userId);
        authorizableNode.setProperty(REP_AUTHORIZABLE_ID, authorizableId);
        authorizableNode.setProperty(JcrConstants.JCR_UUID, getContentID(authorizableId));

        return authorizableNode;
    }

    /**
     * Create folder structure for the authorizable to be created. The structure
     * consists of a tree of rep:AuthorizableFolder node(s) starting at the
     * configured user or group path. Note that Authorizable nodes are never
     * nested.
     *
     * @param nodeName         The name of the authorizable node.
     * @param isGroup          Flag indicating whether the new authorizable is a group or a user.
     * @param intermediatePath An optional intermediate path.
     * @return The folder node.
     * @throws RepositoryException If an error occurs
     */
    private Tree createFolderNodes(@Nonnull String nodeName,
                                       boolean isGroup,
                                       @Nullable String intermediatePath) throws RepositoryException {
        String authRoot = (isGroup) ? groupPath : userPath;
        String folderPath = new StringBuilder()
                .append(authRoot)
                .append(getFolderPath(nodeName, intermediatePath, authRoot)).toString();
        Tree folder;
        Tree tree = root.getTree(folderPath);
        while (!tree.isRoot() && !tree.exists()) {
            tree = tree.getParent();
        }
        if (tree.exists()) {
            folder = tree;
            String relativePath = PathUtils.relativize(tree.getPath(), folderPath);
            if (!relativePath.isEmpty()) {
                folder = Utils.getOrAddTree(folder, relativePath, NT_REP_AUTHORIZABLE_FOLDER);
            }
        } else {
            throw new AccessDeniedException("Missing permission to create intermediate authorizable folders.");
        }

        // test for colliding folder child node.
        while (folder.hasChild(nodeName)) {
            Tree colliding = folder.getChild(nodeName);
            String primaryType = TreeUtil.getPrimaryTypeName(colliding);
            if (NT_REP_AUTHORIZABLE_FOLDER.equals(primaryType)) {
                log.debug("Existing folder node collides with user/group to be created. Expanding path by: " + colliding.getName());
                folder = colliding;
            } else {
                break;
            }
        }
        return folder;
    }

    @Nonnull
    private String getFolderPath(@Nonnull String nodeName,
                                 @Nullable String intermediatePath,
                                 @Nonnull String authRoot) throws ConstraintViolationException {
        boolean emptyOrNull = (intermediatePath == null || intermediatePath.isEmpty() || authRoot.equals(intermediatePath));
        StringBuilder sb = new StringBuilder();
        if (!emptyOrNull) {
            // convert absolute paths into relative paths wrt the authRoot
            if (intermediatePath.charAt(0) == '/') {
                if (!intermediatePath.startsWith(authRoot)) {
                    throw new ConstraintViolationException("Attempt to create authorizable at '" + intermediatePath +"' outside of the configured root '" + authRoot + '\'');
                } else {
                    intermediatePath = intermediatePath.substring(authRoot.length() + 1);
                }
            }
            sb.append(DELIMITER).append(intermediatePath);
        } else {
            String hint = Text.unescapeIllegalJcrChars(nodeName);
            int idLength = hint.length();
            StringBuilder segment = new StringBuilder();
            for (int i = 0; i < defaultDepth; i++) {
                if (idLength > i) {
                    segment.append(hint.charAt(i));
                } else {
                    // escapedID is too short -> append the last char again
                    segment.append(hint.charAt(idLength - 1));
                }
                sb.append(DELIMITER).append(Text.escapeIllegalJcrChars(segment.toString()));
            }
        }
        return sb.toString();
    }

    private String getNodeName(@Nonnull String authorizableId) {
        AuthorizableNodeName generator = checkNotNull(config.getConfigValue(PARAM_AUTHORIZABLE_NODE_NAME, AuthorizableNodeName.DEFAULT, AuthorizableNodeName.class));
        return generator.generateNodeName(authorizableId);
    }
}