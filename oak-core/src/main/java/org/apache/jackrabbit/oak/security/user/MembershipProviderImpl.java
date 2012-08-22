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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.commons.iterator.RangeIteratorAdapter;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.user.MembershipProvider;
import org.apache.jackrabbit.oak.spi.security.user.Type;
import org.apache.jackrabbit.oak.spi.security.user.UserConfig;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MembershipProviderImpl... TODO
 */
public class MembershipProviderImpl extends AuthorizableBaseProvider implements MembershipProvider {

    private static final Logger log = LoggerFactory.getLogger(MembershipProviderImpl.class);

    private final int splitSize;

    MembershipProviderImpl(ContentSession contentSession, Root root, UserConfig config) {
        super(contentSession, root, config);

        int splitValue = config.getConfigValue(UserConfig.PARAM_GROUP_MEMBERSHIP_SPLIT_SIZE, 0);
        if (splitValue != 0 && splitValue < 4) {
            log.warn("Invalid value {} for {}. Expected integer >= 4 or 0", splitValue, UserConfig.PARAM_GROUP_MEMBERSHIP_SPLIT_SIZE);
            splitValue = 0;
        }
        this.splitSize = splitValue;
    }

    //--------------------------------------------------< MembershipProvider>---
    @Override
    public Iterator<String> getMembership(String authorizableId, boolean includeInherited) {
        return getMembership(getByID(authorizableId, Type.AUTHORIZABLE), includeInherited);
    }

    @Override
    public Iterator<String> getMembership(Tree authorizableTree, boolean includeInherited) {
        Set<String> groupPaths = new HashSet<String>();
        Set<String> refPaths = identifierManager.getReferences(true, authorizableTree, null, NT_REP_GROUP, NT_REP_MEMBERS);
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
    public Iterator<String> getMembers(String groupId, Type authorizableType, boolean includeInherited) {
        Tree groupTree = getByID(groupId, Type.GROUP);
        if (groupTree == null) {
            return Iterators.emptyIterator();
        } else {
            return getMembers(groupTree, authorizableType, includeInherited);
        }
    }

    @Override
    public Iterator<String> getMembers(Tree groupTree, Type authorizableType, boolean includeInherited) {
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
            NodeUtil groupNode = new NodeUtil(groupTree, valueFactory);
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

    //-----------------------------------------< private MembershipProvider >---

    private CoreValue createCoreValue(Tree authorizableTree) {
        return valueFactory.createValue(getContentID(authorizableTree), PropertyType.WEAKREFERENCE);
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
                                           final Type authorizableType) {
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
                Tree group = getByPath(authorizablePath);
                if (isAuthorizableTree(group, Type.GROUP)) {
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
                Tree group = getByPath(authorizablePath);
                if (isAuthorizableTree(group, Type.GROUP)) {
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