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
package org.apache.jackrabbit.oak.jcr.security.user;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.jcr.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.value.ValueConverter;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.ConstraintViolationException;
import java.io.UnsupportedEncodingException;
import java.util.UUID;

/**
 * Utility class creating the JCR nodes corresponding the a given
 * authorizable ID with the following behavior:
 * <ul>
 * <li>Users are created below /home/users or
 * the corresponding path configured.</li>
 * <li>Groups are created below /home/groups or
 * the corresponding path configured.</li>
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
 * <li>In case of a user the node name is calculated from the specified UserID
 * {@link Text#escapeIllegalJcrChars(String) escaping} any illegal JCR chars.
 * In case of a Group the node name is calculated from the specified principal
 * name circumventing any conflicts with existing ids and escaping illegal chars.</li>
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
 * </pre>
 * </li>
 * <li>If the {@code autoExpandTree} option is {@code true} the
 * user tree will be automatically expanded using additional levels if
 * {@code autoExpandSize} is exceeded within a given level.</li>
 * </ul>
 *
 * The auto-expansion of the authorizable tree is defined by the following
 * steps and exceptional cases:
 * <ul>
 * <li>As long as {@code autoExpandSize} isn't reached authorizable
 * nodes are created within the structure defined by the
 * {@code defaultDepth}. (see above)</li>
 * <li>If {@code autoExpandSize} is reached additional intermediate
 * folders will be created.<br>
 * E.g. creating a user node for an ID 'aSmith1001' would result in the
 * following structure:
 * <pre>
 * + rep:security            [rep:AuthorizableFolder]
 *   + rep:authorizables     [rep:AuthorizableFolder]
 *     + rep:users           [rep:AuthorizableFolder]
 *       + a                 [rep:AuthorizableFolder]
 *         + aS              [rep:AuthorizableFolder]
 *           + aSmith1       [rep:User]
 *           + aSmith2       [rep:User]
 *           [...]
 *           + aSmith1000    [rep:User]
 * ->        + aSm           [rep:AuthorizableFolder]
 * ->          + aSmith1001  [rep:User]
 * </pre>
 * </li>
 * <li>Conflicts: In order to prevent any conflicts that would arise from
 * creating a authorizable node that upon later expansion could conflict
 * with an authorizable folder, intermediate levels are always created if
 * the node name equals any of the names reserved for the next level of
 * folders.<br>
 * In the example above any attempt to create a user with ID 'aSm' would
 * result in an intermediate level irrespective if max-size has been
 * reached or not:
 * <pre>
 * + rep:security            [rep:AuthorizableFolder]
 *   + rep:authorizables     [rep:AuthorizableFolder]
 *     + rep:users           [rep:AuthorizableFolder]
 *       + a                 [rep:AuthorizableFolder]
 *         + aS              [rep:AuthorizableFolder]
 * ->        + aSm           [rep:AuthorizableFolder]
 * ->          + aSm         [rep:User]
 * </pre>
 * </li>
 * <li>Special case: If the name of the authorizable node to be created is
 * shorter or equal to the length of the folder at level N, the authorizable
 * node is created even if max-size has been reached before.<br>
 * An attempt to create the users 'aS' and 'aSm' in a structure containing
 * tons of 'aSmith' users will therefore result in:
 * <pre>
 * + rep:security            [rep:AuthorizableFolder]
 *   + rep:authorizables     [rep:AuthorizableFolder]
 *     + rep:users           [rep:AuthorizableFolder]
 *       + a                 [rep:AuthorizableFolder]
 *         + aS              [rep:AuthorizableFolder]
 *           + aSmith1       [rep:User]
 *           + aSmith2       [rep:User]
 *           [...]
 *           + aSmith1000    [rep:User]
 * ->        + aS            [rep:User]
 *           + aSm           [rep:AuthorizableFolder]
 *             + aSmith1001  [rep:User]
 * ->          + aSm         [rep:User]
 * </pre>
 * </li>
 * <li>Special case: If {@code autoExpandTree} is enabled later on
 * AND any of the existing authorizable nodes collides with an intermediate
 * folder to be created the auto-expansion is aborted and the new
 * authorizable is inserted at the last valid level irrespective of
 * max-size being reached.
 * </li>
 * </ul>
 *
 * The configuration options:
 * <ul>
 * <li><strong>defaultDepth</strong>:<br>
 * A positive {@code integer} greater than zero defining the depth of
 * the default structure that is always created.<br>
 * Default value: 2</li>
 * <li><strong>autoExpandTree</strong>:<br>
 * {@code boolean} defining if the tree gets automatically expanded
 * if within a level the maximum number of child nodes is reached.<br>
 * Default value: {@code false}</li>
 * <li><strong>autoExpandSize</strong>:<br>
 * A positive {@code long} greater than zero defining the maximum
 * number of child nodes that are allowed at a given level.<br>
 * Default value: 1000<br>
 * NOTE: that total number of child nodes may still be greater that
 * autoExpandSize.</li>
 * </ul>
 */
class AuthorizableNodeCreator {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(AuthorizableNodeCreator.class);

    private static final String DELIMITER = "/";
    private static final int DEFAULT_DEPTH = 2;
    private static final int DEFAULT_SIZE = 1000;

    private final SessionDelegate sessionDelegate;

    private final int defaultDepth;
    private final boolean autoExpandTree;
    private final long autoExpandSize;

    private final String groupPath;
    private final String userPath;

    private final String ntAuthorizableFolder;
    private final String ntAuthorizable;

    AuthorizableNodeCreator(SessionDelegate sessionDelegate, UserManagerConfig config) {
        this.sessionDelegate = sessionDelegate;

        defaultDepth = config.getConfigValue(UserManagerConfig.PARAM_DEFAULT_DEPTH, DEFAULT_DEPTH);
        autoExpandTree = config.getConfigValue(UserManagerConfig.PARAM_AUTO_EXPAND_TREE, false);
        autoExpandSize = config.getConfigValue(UserManagerConfig.PARAM_AUTO_EXPAND_SIZE, DEFAULT_SIZE);

        groupPath = config.getConfigValue(UserManagerConfig.PARAM_GROUP_PATH, "/rep:security/rep:authorizables/rep:groups");
        userPath = config.getConfigValue(UserManagerConfig.PARAM_USER_PATH, "/rep:security/rep:authorizables/rep:users");

        NamePathMapper namePathMapper = sessionDelegate.getNamePathMapper();
        ntAuthorizableFolder = namePathMapper.getJcrName(UserConstants.NT_REP_AUTHORIZABLE_FOLDER);
        ntAuthorizable = namePathMapper.getJcrName(UserConstants.NT_REP_AUTHORIZABLE);
    }

    String getNodeID(String authorizableId) throws RepositoryException {
        try {
            UUID uuid = UUID.nameUUIDFromBytes(authorizableId.toLowerCase().getBytes("UTF-8"));
            return uuid.toString();
        } catch (UnsupportedEncodingException e) {
            throw new RepositoryException("Unexpected error while creating authorizable node", e);
        }
    }

    Node createUserNode(String userID, String intermediatePath) throws RepositoryException {
        return createAuthorizableNode(userID, false, intermediatePath);
    }

    Node createGroupNode(String groupID, String intermediatePath) throws RepositoryException {
        return createAuthorizableNode(groupID, true, intermediatePath);
    }

    private Node createAuthorizableNode(String authorizableId, boolean isGroup, String intermediatePath) throws RepositoryException {
        String nodeName = Text.escapeIllegalJcrChars(authorizableId);
        Node folder = createFolderNodes(authorizableId, nodeName, isGroup, intermediatePath);

        String ntName = (isGroup) ? UserConstants.NT_REP_GROUP : UserConstants.NT_REP_USER;
        Node authorizableNode = folder.addNode(nodeName, ntName);

        String nodeID = getNodeID(authorizableId);
        CoreValue idValue = ValueConverter.toCoreValue(nodeID, PropertyType.STRING, sessionDelegate);
        sessionDelegate.getNode(authorizableNode).setProperty(JcrConstants.JCR_UUID, idValue);

        return folder.getNode(nodeName);
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
     * @return
     * @throws RepositoryException
     */
    private Node createFolderNodes(String authorizableId, String nodeName,
                                   boolean isGroup, String intermediatePath) throws RepositoryException {
        Session session = sessionDelegate.getSession();
        String authRoot = (isGroup) ? groupPath : userPath;
        Node folder;
        if (!session.nodeExists(authRoot)) {
            folder = session.getRootNode();
            for (String name : Text.explode(authRoot, '/', false)) {
                if (folder.hasNode(name)) {
                    folder = folder.getNode(name);
                } else {
                    folder = folder.addNode(name, ntAuthorizableFolder);
                }
            }
        } else {
            folder = session.getNode(authRoot);
        }
        String folderPath = getFolderPath(authorizableId, intermediatePath);
        String[] segmts = Text.explode(folderPath, '/', false);
        for (String segment : segmts) {
            if (folder.hasNode(segment)) {
                folder = folder.getNode(segment);
                if (!folder.isNodeType(ntAuthorizableFolder)) {
                    throw new ConstraintViolationException("Cannot create user/group: Intermediate folders must be of type rep:AuthorizableFolder.");
                }
            } else {
                folder = folder.addNode(segment, ntAuthorizableFolder);
            }
        }

        if (intermediatePath == null && autoExpandTree) {
            folder = expandTree(authorizableId, nodeName, folder);
        }

        // test for colliding folder child node.
        while (folder.hasNode(nodeName)) {
            Node colliding = folder.getNode(nodeName);
            if (colliding.isNodeType(UserConstants.NT_REP_AUTHORIZABLE_FOLDER)) {
                log.debug("Existing folder node collides with user/group to be created. Expanding path: " + colliding.getPath());
                folder = colliding;
            } else {
                String msg = "Failed to create authorizable with id '" + authorizableId + "' : Detected conflicting node of unexpected node type '" + colliding.getPrimaryNodeType().getName() + "'.";
                log.error(msg);
                throw new ConstraintViolationException(msg);
            }
        }

        if (!Text.isDescendantOrEqual(authRoot, folder.getPath())) {
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
            for (int i = 0; i < defaultDepth; i++) {
                char c;
                if (idLength > i) {
                    c = authorizableId.charAt(i);
                } else {
                    // escapedID is too short -> append the last char again
                    c = authorizableId.charAt(idLength-1);
                }
                sb.append(DELIMITER).append(Text.escapeIllegalJcrChars(String.valueOf(c)));
            }
        }
        return sb.toString();
    }

    /**
     * Expand the tree structure adding additional folders if any of the
     * following conditions is met:
     * <ul>
     *     <li>number of child node exceeds the configured max value</li>
     *     <li>the authorizable node collides with an intermediate folder</li>
     * </ul>
     *
     * @param authorizableId The authorizable id
     * @param nodeName The name of the authorizable node.
     * @param folder The folder node.
     * @return The node in the authorizable folder tree underneath with the
     * authorizable node will be created.
     * @throws RepositoryException If an error occurs.
     */
    private Node expandTree(String authorizableId, String nodeName, Node folder) throws RepositoryException {
        int segmLength = defaultDepth +1;
        while (isExpand(folder, nodeName.length())) {
            String folderName = Text.escapeIllegalJcrChars(authorizableId.substring(0, segmLength));
            if (folder.hasNode(folderName)) {
                Node n = folder.getNode(folderName);
                // assert that the folder is of type rep:AuthorizableFolder
                if (n.isNodeType(ntAuthorizableFolder)) {
                    folder = n;
                } else if (n.isNodeType(ntAuthorizable)){
                    /*
                     an authorizable node has been created before with the
                     name of the intermediate folder to be created.
                     this may only occur if the 'autoExpandTree' option has
                     been enabled later on.
                     Resolution:
                     - abort auto-expanding and create the authorizable
                       at the current level, ignoring that max-size is reached.
                     - note, that this behavior has been preferred over tmp.
                       removing and recreating the colliding authorizable node.
                    */
                    log.warn("Auto-expanding aborted. An existing authorizable node '" + n.getName() +"' conflicts with intermediate folder to be created.");
                    break;
                } else {
                    // should never get here: some other, unexpected node type
                    String msg = "Failed to create authorizable node: Detected conflict with node of unexpected nodetype '" + n.getPrimaryNodeType().getName() + "'.";
                    log.error(msg);
                    throw new ConstraintViolationException(msg);
                }
            } else {
                // folder doesn't exist nor does another colliding child node.
                folder = folder.addNode(folderName, ntAuthorizable);
            }
            segmLength++;
        }
        return folder;
    }

    private boolean isExpand(Node folder, int nameLength) throws RepositoryException {
        int folderNameLength = folder.getName().length();
        // don't create additional intermediate folders for ids that are
        // shorter or equally long as the folder name. In this case the
        // MAX_SIZE flag is ignored.
        if (nameLength <= folderNameLength) {
            return false;
        }

        // test for potential (or existing) collision in which case the
        // intermediate node is created irrespective of the MAX_SIZE and the
        // existing number of children.
        if (nameLength == folderNameLength+1) {
            // max-size may not yet be reached yet on folder but the node to
            // be created potentially collides with an intermediate folder.
            // e.g.:
            // existing folder structure: a/ab
            // authID to be created     : abt
            // OR
            // existing collision that would result from
            // existing folder structure: a/ab/abt
            // authID to be create      : abt
            return true;
        }

        // last possibility: max-size is reached.
        if (folder.getNodes().getSize() >= autoExpandSize) {
            return true;
        }

        // no collision and no need to create an additional intermediate
        // folder due to max-size reached
        return false;
    }
}