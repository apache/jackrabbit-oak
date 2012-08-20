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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.query.Query;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.iterator.RangeIteratorAdapter;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.SessionQueryEngine;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.spi.security.principal.TreeBasedPrincipal;
import org.apache.jackrabbit.oak.spi.security.user.MembershipProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserManagerConfig;
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
public class UserProviderImpl implements UserProvider, MembershipProvider, UserConstants {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(UserProviderImpl.class);

    private static final String DELIMITER = "/";
    private static final int DEFAULT_DEPTH = 2;

    private final ContentSession contentSession;
    private final Root root;
    private final IdentifierManager identifierManager;

    private final int defaultDepth;
    private final int splitSize;
    private final String adminId;

    private final String groupPath;
    private final String userPath;

    public UserProviderImpl(ContentSession contentSession, Root root, UserManagerConfig config) {
        this.contentSession = contentSession;
        this.root = root;
        this.identifierManager = new IdentifierManager(contentSession.getQueryEngine(), root);

        defaultDepth = config.getConfigValue(UserManagerConfig.PARAM_DEFAULT_DEPTH, DEFAULT_DEPTH);
        int splitValue = config.getConfigValue(UserManagerConfig.PARAM_GROUP_MEMBERSHIP_SPLIT_SIZE, 0);
        if (splitValue != 0 && splitValue < 4) {
            log.warn("Invalid value {} for {}. Expected integer >= 4 or 0", splitValue, UserManagerConfig.PARAM_GROUP_MEMBERSHIP_SPLIT_SIZE);
            splitValue = 0;
        }
        this.splitSize = splitValue;
        this.adminId = config.getAdminId();

        groupPath = config.getConfigValue(UserManagerConfig.PARAM_GROUP_PATH, DEFAULT_GROUP_PATH);
        userPath = config.getConfigValue(UserManagerConfig.PARAM_USER_PATH, DEFAULT_USER_PATH);
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
        Tree tree = identifierManager.getTree(getContentID(authorizableId));
        if (isAuthorizableTree(tree, UserManager.SEARCH_TYPE_AUTHORIZABLE)) {
            return tree;
        } else {
            return null;
        }
    }

    @Override
    public Tree getAuthorizable(String authorizableId, int authorizableType) {
        Tree tree = identifierManager.getTree(getContentID(authorizableId));
        if (isAuthorizableTree(tree, authorizableType)) {
            return tree;
        } else {
            return null;
        }
    }

    @Override()
    public Tree getAuthorizableByPath(String authorizableOakPath) {
        Tree tree = root.getTree(authorizableOakPath);
        if (isAuthorizableTree(tree, UserManager.SEARCH_TYPE_AUTHORIZABLE)) {
            return tree;
        } else {
            return null;
        }
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
            SessionQueryEngine queryEngine = contentSession.getQueryEngine();
            try {
                CoreValue bindValue = contentSession.getCoreValueFactory().createValue(principal.getName());
                Map<String, CoreValue> bindings = Collections.singletonMap("principalName", bindValue);
                String stmt = "SELECT * FROM [rep:Authorizable] WHERE [rep:principalName] = $principalName";
                Result result = contentSession.getQueryEngine().executeQuery(stmt,
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
    public String getAuthorizableId(Tree authorizableTree) {
        assert authorizableTree != null;
        PropertyState idProp = authorizableTree.getProperty(UserConstants.REP_AUTHORIZABLE_ID);
        if (idProp != null) {
            return idProp.getValue().getString();
        } else {
            return Text.unescapeIllegalJcrChars(authorizableTree.getName());
        }
    }

    @Override
    public boolean isAdminUser(Tree userTree) {
        assert userTree != null;
        return isAuthorizableTree(userTree, UserManager.SEARCH_TYPE_USER) &&
               adminId.equals(getAuthorizableId(userTree));
    }

    //--------------------------------------------------< MembershipProvider>---
    @Override
    public Iterator<String> getMembership(String authorizableId, boolean includeInherited) {
        return getMembership(getAuthorizable(authorizableId), includeInherited);
    }

    @Override
    public Iterator<String> getMembership(Tree authorizableTree, boolean includeInherited) {
        Set<String> groupPaths = new HashSet<String>();
        Set<String> refPaths = identifierManager.getWeakReferences(authorizableTree, null, NT_REP_GROUP, NT_REP_MEMBERS);
        for (String propPath : refPaths) {
            int index = propPath.indexOf('/'+REP_MEMBERS);
            if (index > 0) {
                groupPaths.add(propPath.substring(0, index));
            } else {
                log.debug("Not a membership reference property " + propPath);
            }
        }

        Iterator<String> it = groupPaths.iterator();
        if (includeInherited && it.hasNext()) {
            return getAllMembership(groupPaths.iterator());
        } else {
            return new RangeIteratorAdapter(it, groupPaths.size());
        }
    }

    @Override
    public Iterator<String> getMembers(String groupId, int authorizableType, boolean includeInherited) {
        return getMembers(getAuthorizable(groupId), UserManager.SEARCH_TYPE_AUTHORIZABLE, includeInherited);
    }

    @Override
    public Iterator<String> getMembers(Tree groupTree, int authorizableType, boolean includeInherited) {
        Iterable memberPaths = Collections.emptySet();
        if (useMemberNode(groupTree)) {
            Tree membersTree = groupTree.getChild(REP_MEMBERS);
            if (membersTree != null) {
                // FIXME: replace usage of PropertySequence (oak-api not possible there)
//                PropertySequence propertySequence = getPropertySequence(membersTree);
//                iterator = new AuthorizableIterator(propertySequence, authorizableType, userManager);
            }
        } else {
            PropertyState property = groupTree.getProperty(REP_MEMBERS);
            if (property != null) {
                List<CoreValue> vs = property.getValues();
                memberPaths = Iterables.transform(vs, new Function<CoreValue,String>() {
                    @Override
                    public String apply(@Nullable CoreValue value) {
                        return identifierManager.getPath(value);
                    }
                });
            }
        }

        Iterator it = memberPaths.iterator();
        if (includeInherited && it.hasNext()) {
            return getAllMembers(it, authorizableType);
        } else {
            return new RangeIteratorAdapter(it, Iterables.size(memberPaths));
        }
    }

    @Override
    public boolean isMember(Tree groupTree, Tree authorizableTree, boolean includeInherited) {
        if (includeInherited) {
            Iterator<String> groupPaths = getMembership(authorizableTree, true);
            String path = groupTree.getPath();
            while (groupPaths.hasNext()) {
                if (path.equals(groupPaths.next())) {
                    return true;
                }
            }
        } else {
            if (useMemberNode(groupTree)) {
                Tree membersTree = groupTree.getChild(REP_MEMBERS);
                if (membersTree != null) {
                    // FIXME: fix.. testing for property name isn't correct.
                    // FIXME: usage of PropertySequence isn't possible when operating on oak-API
//                    PropertySequence propertySequence = getPropertySequence(membersTree);
//                    return propertySequence.hasItem(authorizable.getID());
                    return false;
                }
            } else {
                PropertyState property = groupTree.getProperty(REP_MEMBERS);
                if (property != null) {
                    List<CoreValue> members = property.getValues();
                    String authorizableUUID = getContentID(authorizableTree);
                    for (CoreValue v : members) {
                        if (authorizableUUID.equals(v.getString())) {
                            return true;
                        }
                    }
                }
            }
        }
        // no a member of the specified group
        return false;
    }

    @Override
    public boolean addMember(Tree groupTree, Tree newMemberTree) {
        if (useMemberNode(groupTree)) {
            NodeUtil groupNode = new NodeUtil(groupTree, contentSession);
            NodeUtil membersNode = groupNode.getOrAddChild(REP_MEMBERS, NT_REP_MEMBERS);

            //FIXME: replace usage of PropertySequence with oak-compatible utility
//            PropertySequence properties = getPropertySequence(membersTree);
//            String propName = Text.escapeIllegalJcrChars(authorizable.getID());
//            if (properties.hasItem(propName)) {
//                log.debug("Authorizable {} is already member of {}", authorizable, this);
//                return false;
//            } else {
//                CoreValue newMember = createCoreValue(authorizable);
//                properties.addProperty(propName, newMember);
//            }
        } else {
            List<CoreValue> values;
            CoreValue toAdd = createCoreValue(newMemberTree);
            PropertyState property = groupTree.getProperty(REP_MEMBERS);
            if (property != null) {
                values = property.getValues();
                if (values.contains(toAdd)) {
                    return false;
                } else {
                    values.add(toAdd);
                }
            } else {
                values = Collections.singletonList(toAdd);
            }
            groupTree.setProperty(REP_MEMBERS, values);
        }
        return true;
    }

    @Override
    public boolean removeMember(Tree groupTree, Tree memberTree) {
        if (useMemberNode(groupTree)) {
            Tree membersTree = groupTree.getChild(REP_MEMBERS);
            if (membersTree != null) {
                // FIXME: replace usage of PropertySequence with oak-compatible utility
//                PropertySequence properties = getPropertySequence(membersTree);
//                String propName = authorizable.getTree().getName();
                // FIXME: fix.. testing for property name isn't correct.
//                if (properties.hasItem(propName)) {
//                    Property p = properties.getItem(propName);
//                    userManager.removeInternalProperty(p.getParent(), propName);
//                }
//                return true;
                return false;
            }
        } else {
            PropertyState property = groupTree.getProperty(REP_MEMBERS);
            if (property != null) {
                CoreValue toRemove = createCoreValue(memberTree);
                List<CoreValue> values = property.getValues();
                if (values.remove(toRemove)) {
                    if (values.isEmpty()) {
                        groupTree.removeProperty(REP_MEMBERS);
                    } else {
                        groupTree.setProperty(REP_MEMBERS, values);
                    }
                    return true;
                }
            }
        }

        // nothing changed
        log.debug("Authorizable {} was not member of {}", memberTree.getName(), groupTree.getName());
        return false;
    }

    //------------------------------------------------------------< private >---

    private String getContentID(String authorizableId) {
        return IdentifierManager.generateUUID(authorizableId.toLowerCase());
    }

    private String getContentID(Tree authorizableTree) {
        return identifierManager.getIdentifier(authorizableTree);
    }

    private boolean isAuthorizableTree(Tree tree, int type) {
        // FIXME: check for node type according to the specified type constraint
        if (tree != null && tree.hasProperty(JcrConstants.JCR_PRIMARYTYPE)) {
            String ntName = tree.getProperty(JcrConstants.JCR_PRIMARYTYPE).getValue().getString();
            switch (type) {
                case UserManager.SEARCH_TYPE_GROUP:
                    return NT_REP_GROUP.equals(ntName);
                case UserManager.SEARCH_TYPE_USER:
                    return NT_REP_USER.equals(ntName);
                default:
                    return NT_REP_USER.equals(ntName) || NT_REP_GROUP.equals(ntName);
            }
        }
        return false;
    }

    //-----------------------------------------------< private UserProvider >---

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
            folder = new NodeUtil(root.getTree("/"), contentSession);
            for (String name : Text.explode(authRoot, '/', false)) {
                folder = folder.getOrAddChild(name, NT_REP_AUTHORIZABLE_FOLDER);
            }
        }  else {
            folder = new NodeUtil(authTree, contentSession);
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

    //-----------------------------------------< private MembershipProvider >---

    private CoreValue createCoreValue(Tree authorizableTree) {
        return contentSession.getCoreValueFactory().createValue(getContentID(authorizableTree), PropertyType.WEAKREFERENCE);
    }

    private boolean useMemberNode(Tree groupTree) {
        return splitSize >= 4 && !groupTree.hasProperty(REP_MEMBERS);
    }

    /**
     * Returns an iterator of authorizables which includes all indirect members
     * of the given iterator of authorizables.
     *
     *
     * @param declaredMembers
     * @param authorizableType
     * @return Iterator of Authorizable objects
     */
    private Iterator<String> getAllMembers(final Iterator<String> declaredMembers,
                                           final int authorizableType) {
        Iterator<Iterator<String>> inheritedMembers = new Iterator<Iterator<String>>() {
            @Override
            public boolean hasNext() {
                return declaredMembers.hasNext();
            }

            @Override
            public Iterator<String> next() {
                String memberPath = declaredMembers.next();
                return Iterators.concat(Iterators.singletonIterator(memberPath), inherited(memberPath));
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private Iterator<String> inherited(String authorizablePath) {
                Tree group = getAuthorizableByPath(authorizablePath);
                if (isAuthorizableTree(group, UserManager.SEARCH_TYPE_GROUP)) {
                    return getMembers(group, authorizableType, true);
                } else {
                    return Iterators.emptyIterator();
                }
            }
        };
        return Iterators.filter(Iterators.concat(inheritedMembers), new ProcessedPathPredicate());
    }

    private Iterator<String> getAllMembership(final Iterator<String> groupPaths) {
        Iterator<Iterator<String>> inheritedMembership = new Iterator<Iterator<String>>() {
            @Override
            public boolean hasNext() {
                return groupPaths.hasNext();
            }

            @Override
            public Iterator<String> next() {
                String groupPath = groupPaths.next();
                return Iterators.concat(Iterators.singletonIterator(groupPath), inherited(groupPath));
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private Iterator<String> inherited(String authorizablePath) {
                Tree group = getAuthorizableByPath(authorizablePath);
                if (isAuthorizableTree(group, UserManager.SEARCH_TYPE_GROUP)) {
                    return getMembership(group, true);
                } else {
                    return Iterators.emptyIterator();
                }
            }
        };

        return Iterators.filter(Iterators.concat(inheritedMembership), new ProcessedPathPredicate());
    }

    private static final class ProcessedPathPredicate implements Predicate<String> {
        private final Set<String> processed = new HashSet<String>();
        @Override
        public boolean apply(@Nullable String path) {
            return processed.add(path);
        }
    }
}