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

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.flat.BTreeManager;
import org.apache.jackrabbit.commons.flat.ItemSequence;
import org.apache.jackrabbit.commons.flat.PropertySequence;
import org.apache.jackrabbit.commons.flat.Rank;
import org.apache.jackrabbit.commons.flat.TreeManager;
import org.apache.jackrabbit.commons.iterator.RangeIteratorAdapter;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MembershipManager...
 */
class MembershipManager implements UserConstants {

    private static final Logger log = LoggerFactory.getLogger(MembershipManager.class);

    private final UserManagerImpl userManager;
    private final int memberSplitSize;

    MembershipManager(UserManagerImpl userManager, int memberSplitSize) {
        this.userManager = userManager;
        this.memberSplitSize = memberSplitSize;
    }

    Iterator<Group> getMembership(AuthorizableImpl authorizable, boolean includeInherited) throws RepositoryException {
        Set<String> groupPaths = new HashSet<String>();

        String nodeID = authorizable.getContentID();
        IdentifierManager idManager = userManager.getSessionDelegate().getIdManager();
        NamePathMapper mapper = userManager.getSessionDelegate().getNamePathMapper();

        Set<String> refPaths = idManager.getWeakReferences(authorizable.getTree(), null, NT_REP_GROUP, NT_REP_MEMBERS);
        for (String propPath : refPaths) {
            int index = propPath.indexOf('/'+REP_MEMBERS);
            if (index > 0) {
                groupPaths.add(mapper.getJcrPath(propPath.substring(0,index)));
            } else {
                log.debug("Not a membership reference property " + propPath);
            }
        }

        if (!groupPaths.isEmpty()) {
            AuthorizableIterator iterator = new AuthorizableIterator(groupPaths, UserManager.SEARCH_TYPE_GROUP, userManager);
            if (includeInherited) {
                return getAllMembership(iterator);
            } else {
                return new RangeIteratorAdapter(iterator, iterator.getSize());
            }
        } else {
            return RangeIteratorAdapter.EMPTY;
        }

    }

    boolean isMember(GroupImpl group, AuthorizableImpl authorizable, boolean includeInherited) throws RepositoryException {
        Tree groupTree = group.getTree();

        if (includeInherited) {
            Iterator<Group> groups = getMembership(authorizable, true);
            String id = group.getID();
            while (groups.hasNext()) {
                if (id.equals(groups.next().getID())) {
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
                    for (CoreValue v : members) {
                        if (authorizable.getContentID().equals(v.getString())) {
                            return true;
                        }
                    }
                }
            }
        }
        // no a member of the specified group
        return false;
    }

    Iterator<Authorizable> getMembers(GroupImpl group, int authorizableType,
                                      boolean includeInherited) throws RepositoryException {
        Tree groupTree = group.getTree();
        AuthorizableIterator iterator = null;

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
                List<CoreValue> members = property.getValues();
                iterator = new AuthorizableIterator(members, authorizableType, userManager);
            }
        }

        if (iterator != null) {
            if (includeInherited) {
                return getAllMembers(iterator, authorizableType);
            } else {
                return new RangeIteratorAdapter(iterator, iterator.getSize());
            }
        } else {
            return RangeIteratorAdapter.EMPTY;
        }
    }

    boolean addMember(GroupImpl group, AuthorizableImpl authorizable) throws RepositoryException {
        Tree groupTree = group.getTree();
        if (useMemberNode(groupTree)) {
            Tree membersTree = groupTree.getChild(REP_MEMBERS);
            if (membersTree == null) {
                membersTree = groupTree.addChild(REP_MEMBERS);
                membersTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, getCoreValueFactory().createValue(NT_REP_MEMBERS, PropertyType.NAME));
            }

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
            CoreValue toAdd = createCoreValue(authorizable);

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
            userManager.setInternalProperty(groupTree, REP_MEMBERS, values);
        }
        return true;
    }

    boolean removeMember(GroupImpl group, AuthorizableImpl authorizable) throws RepositoryException {
        Tree groupTree = group.getTree();
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
                CoreValue toRemove = createCoreValue(authorizable);
                List<CoreValue> values = property.getValues();

                if (values.remove(toRemove)) {
                    if (values.isEmpty()) {
                        userManager.removeInternalProperty(groupTree, REP_MEMBERS);
                    } else {
                        userManager.setInternalProperty(groupTree, REP_MEMBERS, values);
                    }
                    return true;
                }
            }
        }

        // nothing changed
        log.debug("Authorizable {} was not member of {}", authorizable.getID(), group.getID());
        return false;
    }

    //--------------------------------------------------------------------------
    private boolean useMemberNode(Tree groupTree) {
        return memberSplitSize >= 4 && !groupTree.hasProperty(REP_MEMBERS);
    }

    // FIXME: replace usage of PropertySequence with utility operating on oak-api
    private PropertySequence getPropertySequence(Node nMembers) throws RepositoryException {
        Comparator<String> order = Rank.comparableComparator();
        int minChildren = memberSplitSize / 2;
        TreeManager treeManager = new BTreeManager(nMembers, minChildren, memberSplitSize, order, false);
        return ItemSequence.createPropertySequence(treeManager);
    }

    /**
     * Returns an iterator of authorizables which includes all indirect members
     * of the given iterator of authorizables.
     *
     * @param authorizables
     * @param authorizableType
     * @return Iterator of Authorizable objects
     */
    private Iterator<Authorizable> getAllMembers(final Iterator<Authorizable> authorizables,
                                                 final int authorizableType) {
        Iterator<Iterator<Authorizable>> inheritedMembers = new Iterator<Iterator<Authorizable>>() {
            @Override
            public boolean hasNext() {
                return authorizables.hasNext();
            }

            @Override
            public Iterator<Authorizable> next() {
                Authorizable next = authorizables.next();
                return Iterators.concat(
                        Iterators.singletonIterator(next), inherited(next));
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private Iterator<Authorizable> inherited(Authorizable authorizable) {
                if (authorizable.isGroup()) {
                    try {
                        return getMembers(((GroupImpl) authorizable), authorizableType, true);
                    } catch (RepositoryException e) {
                        log.warn("Could not determine members of " + authorizable, e);
                    }
                }
                return Iterators.emptyIterator();
            }
        };

        return new InheritingAuthorizableIterator(inheritedMembers);
    }

    private Iterator<Group> getAllMembership(final AuthorizableIterator membership) {
        Iterator<Iterator<Group>> inheritedMembership = new Iterator<Iterator<Group>>() {
            @Override
            public boolean hasNext() {
                return membership.hasNext();
            }

            @Override
            public Iterator<Group> next() {
                Group next = (Group) membership.next();
                return Iterators.concat(
                        Iterators.singletonIterator(next),
                        inherited((AuthorizableImpl) next));
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private Iterator<Group> inherited(AuthorizableImpl authorizable) {
                if (authorizable.isGroup()) {
                    try {
                        return getMembership(authorizable, true);
                    } catch (RepositoryException e) {
                        log.warn("Could not determine members of " + authorizable, e);
                    }
                }
                return Iterators.emptyIterator();
            }
        };

        return new InheritingAuthorizableIterator(inheritedMembership);
    }

    private CoreValue createCoreValue(AuthorizableImpl authorizable) throws RepositoryException {
        return getCoreValueFactory().createValue(authorizable.getContentID(), PropertyType.WEAKREFERENCE);
    }

    private CoreValueFactory getCoreValueFactory() {
        return userManager.getSessionDelegate().getContentSession().getCoreValueFactory();
    }
}