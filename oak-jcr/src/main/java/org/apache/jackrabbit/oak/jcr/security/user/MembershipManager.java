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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.flat.BTreeManager;
import org.apache.jackrabbit.commons.flat.ItemSequence;
import org.apache.jackrabbit.commons.flat.PropertySequence;
import org.apache.jackrabbit.commons.flat.Rank;
import org.apache.jackrabbit.commons.flat.TreeManager;
import org.apache.jackrabbit.commons.iterator.RangeIteratorAdapter;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * MembershipManager...
 */
class MembershipManager {

    private static final Logger log = LoggerFactory.getLogger(MembershipManager.class);

    private final UserManagerImpl userManager;
    private final ValueFactory valueFactory;
    private final int memberSplitSize;

    private final String repMembers;

    MembershipManager(UserManagerImpl userManager, int memberSplitSize, ValueFactory valueFactory) {
        this.userManager = userManager;
        this.valueFactory = valueFactory;
        this.memberSplitSize = memberSplitSize;

        repMembers = userManager.getJcrName(UserConstants.REP_MEMBERS);
    }

    Iterator<Group> getMembership(AuthorizableImpl authorizable, boolean includeInherited) throws RepositoryException {
        PropertyIterator refs = null;
        try {
            String nodeID = authorizable.getNode().getIdentifier();
            refs = authorizable.getNode().getWeakReferences(null);
        } catch (RepositoryException e) {
            log.error("Failed to retrieve membership references of " + authorizable.getID(), e);
            // TODO retrieve by traversing
        }

        if (refs != null) {
            AuthorizableIterator iterator = new AuthorizableIterator(refs, UserManager.SEARCH_TYPE_GROUP, userManager);
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
        Node node = group.getNode();

        if (includeInherited) {
            Iterator<Group> groups = getMembership(authorizable, true);
            String id = group.getID();
            while (groups.hasNext()) {
                if (id.equals(groups.next().getID())) {
                    return true;
                }
            }
        } else {
            if (useMemberNode(node)) {
                if (node.hasNode(repMembers)) {
                    // TODO: fix.. testing for property name isn't correct.
                    PropertySequence propertySequence = getPropertySequence(node.getNode(repMembers));
                    return propertySequence.hasItem(authorizable.getID());
                }
            } else {
                if (node.hasProperty(repMembers)) {
                    Value[] members = node.getProperty(repMembers).getValues();
                    for (Value v : members) {
                        if (authorizable.getNode().getIdentifier().equals(v.getString())) {
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
        Node node = group.getNode();
        AuthorizableIterator iterator = null;
        if (useMemberNode(node)) {
            if (node.hasNode(repMembers)) {
                PropertySequence propertySequence = getPropertySequence(node.getNode(repMembers));
                iterator = new AuthorizableIterator(propertySequence, authorizableType, userManager);
            }
        } else {
            if (node.hasProperty(repMembers)) {
                Value[] members = node.getProperty(repMembers).getValues();
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
        Node node = group.getNode();
        if (useMemberNode(node)) {
            // TODO: modify items on oak-api directly
        } else {
            Node memberNode = authorizable.getNode();
            Value[] values;
            Value toAdd = valueFactory.createValue(memberNode, true);
            if (node.hasProperty(repMembers)) {
                Value[] old = node.getProperty(repMembers).getValues();
                values = new Value[old.length + 1];
                System.arraycopy(old, 0, values, 0, old.length);
            } else {
                values = new Value[1];
            }
            values[values.length - 1] = toAdd;

            userManager.setInternalProperty(node, repMembers, values);
        }
        return true;
    }

    boolean removeMember(GroupImpl group, AuthorizableImpl authorizable) throws RepositoryException {
        Node node = group.getNode();

        if (useMemberNode(node)) {
            if (node.hasNode(repMembers)) {
                Node nMembers = node.getNode(repMembers);
                PropertySequence properties = getPropertySequence(nMembers);
                String propName = authorizable.getNode().getName();
                // TODO: fix.. testing for property name isn't correct.
                if (properties.hasItem(propName)) {
                    Property p = properties.getItem(propName);
                    userManager.removeInternalProperty(p.getParent(), propName);
                }
                return true;
            }
        } else {
            if (node.hasProperty(repMembers)) {
                Value toRemove = valueFactory.createValue((authorizable).getNode(), true);
                Property property = node.getProperty(repMembers);
                List<Value> valList = new ArrayList<Value>(Arrays.asList(property.getValues()));

                if (valList.remove(toRemove)) {
                    if (valList.isEmpty()) {
                        userManager.removeInternalProperty(node, repMembers);
                    } else {
                        Value[] values = valList.toArray(new Value[valList.size()]);
                        userManager.setInternalProperty(node, repMembers, values);
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
    private boolean useMemberNode(Node n) throws RepositoryException {
        return memberSplitSize >= 4 && !n.hasProperty(repMembers);
    }

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
}