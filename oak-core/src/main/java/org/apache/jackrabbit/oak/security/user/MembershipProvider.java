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
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.commons.iterator.RangeIteratorAdapter;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.memory.MemoryPropertyBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtility;
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.api.Type.WEAKREFERENCE;

/**
 * {@code MembershipProvider} implementation storing group membership information
 * with the {@code Tree} associated with a given {@link org.apache.jackrabbit.api.security.user.Group}.
 * Depending on the configuration there are two variants on how group members
 * are recorded:
 *
 * <h3>Membership stored in multi-valued property</h3>
 * This is the default way of storing membership information with the following
 * characteristics:
 * <ul>
 *     <li>Multivalued property {@link #REP_MEMBERS}</li>
 *     <li>Property type: {@link PropertyType#WEAKREFERENCE}</li>
 *     <li>Used if the config option {@link org.apache.jackrabbit.oak.spi.security.user.UserConstants#PARAM_GROUP_MEMBERSHIP_SPLIT_SIZE} is missing or &lt;4</li>
 * </ul>
 *
 * <h3>Membership stored in individual properties</h3>
 * Variant to store group membership based on the
 * {@link org.apache.jackrabbit.oak.spi.security.user.UserConstants#PARAM_GROUP_MEMBERSHIP_SPLIT_SIZE} configuration parameter:
 *
 * <ul>
 *     <li>Membership information stored underneath a {@link #REP_MEMBERS} node hierarchy</li>
 *     <li>Individual member information is stored each in a {@link PropertyType#WEAKREFERENCE}
 *     property</li>
 *     <li>Node hierarchy is split based on the {@link org.apache.jackrabbit.oak.spi.security.user.UserConstants#PARAM_GROUP_MEMBERSHIP_SPLIT_SIZE}
 *     configuration parameter.</li>
 *     <li>{@link org.apache.jackrabbit.oak.spi.security.user.UserConstants#PARAM_GROUP_MEMBERSHIP_SPLIT_SIZE} must be greater than 4
 *     in order to turn on this behavior</li>
 * </ul>
 *
 * <h3>Compatibility</h3>
 * This membership provider is able to deal with both options being present in
 * the content. If the {@link org.apache.jackrabbit.oak.spi.security.user.UserConstants#PARAM_GROUP_MEMBERSHIP_SPLIT_SIZE} configuration
 * parameter is modified later on, existing membership information is not
 * modified or converted to the new structure.
 */
class MembershipProvider extends AuthorizableBaseProvider {

    private static final Logger log = LoggerFactory.getLogger(MembershipProvider.class);

    private final int splitSize;

    MembershipProvider(Root root, ConfigurationParameters config) {
        super(root, config);

        int splitValue = config.getConfigValue(PARAM_GROUP_MEMBERSHIP_SPLIT_SIZE, 0);
        if (splitValue != 0 && splitValue < 4) {
            log.warn("Invalid value {} for {}. Expected integer >= 4 or 0", splitValue, PARAM_GROUP_MEMBERSHIP_SPLIT_SIZE);
            splitValue = 0;
        }
        this.splitSize = splitValue;
    }

    @Nonnull
    Iterator<String> getMembership(Tree authorizableTree, boolean includeInherited) {
        Set<String> groupPaths = new HashSet<String>();
        Set<String> refPaths = identifierManager.getReferences(true, authorizableTree, REP_MEMBERS, NT_REP_GROUP, NT_REP_MEMBERS);
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

    @Nonnull
    Iterator<String> getMembers(Tree groupTree, AuthorizableType authorizableType, boolean includeInherited) {
        Iterable memberPaths = Collections.emptySet();
        if (useMemberNode(groupTree)) {
            Tree membersTree = groupTree.getChild(REP_MEMBERS);
            if (membersTree != null) {
                throw new UnsupportedOperationException("not implemented: retrieve members from member-node hierarchy");
            }
        } else {
            PropertyState property = groupTree.getProperty(REP_MEMBERS);
            if (property != null) {
                Iterable<String> vs = property.getValue(STRINGS);
                memberPaths = Iterables.transform(vs, new Function<String, String>() {
                    @Override
                    public String apply(@Nullable String value) {
                        return identifierManager.getPath(PropertyStates.createProperty("", value, WEAKREFERENCE));
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

    boolean isMember(Tree groupTree, Tree authorizableTree, boolean includeInherited) {
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
                    // FIXME: fix.. testing for property name in jr2 wasn't correct.
                    // TODO: add implementation
                    throw new UnsupportedOperationException("not implemented: isMembers determined from member-node hierarchy");
                }
            } else {
                PropertyState property = groupTree.getProperty(REP_MEMBERS);
                if (property != null) {
                    Iterable<String> members = property.getValue(STRINGS);
                    String authorizableUUID = getContentID(authorizableTree);
                    for (String v : members) {
                        if (authorizableUUID.equals(v)) {
                            return true;
                        }
                    }
                }
            }
        }
        // no a member of the specified group
        return false;
    }

    boolean addMember(Tree groupTree, Tree newMemberTree) {
        return addMember(groupTree, newMemberTree.getName(), getContentID(newMemberTree));
    }

    boolean addMember(Tree groupTree, String treeName, String memberContentId) {
        if (useMemberNode(groupTree)) {
            NodeUtil groupNode = new NodeUtil(groupTree);
            NodeUtil membersNode = groupNode.getOrAddChild(REP_MEMBERS, NT_REP_MEMBERS);
            // TODO: add implementation that allows to index group members
            throw new UnsupportedOperationException("not implemented: addMember with member-node hierarchy");
        } else {
            PropertyState property = groupTree.getProperty(REP_MEMBERS);
            PropertyBuilder<String> propertyBuilder = property == null
                ? MemoryPropertyBuilder.create(WEAKREFERENCE, REP_MEMBERS)
                : MemoryPropertyBuilder.create(WEAKREFERENCE, property);
            if (propertyBuilder.hasValue(memberContentId)) {
                return false;
            } else {
                propertyBuilder.addValue(memberContentId);
            }
            groupTree.setProperty(propertyBuilder.getPropertyState(true));
        }
        return true;
    }

    boolean removeMember(Tree groupTree, Tree memberTree) {
        if (useMemberNode(groupTree)) {
            Tree membersTree = groupTree.getChild(REP_MEMBERS);
            if (membersTree != null) {
                // TODO: add implementation
                throw new UnsupportedOperationException("not implemented: remove member from member-node hierarchy");
            }
        } else {
            String toRemove = getContentID(memberTree);
            // FIXME: remove usage of MemoryPropertyBuilder (OAK-372)
            PropertyState property = groupTree.getProperty(REP_MEMBERS);
            PropertyBuilder<String> propertyBuilder = property == null
                ? MemoryPropertyBuilder.create(WEAKREFERENCE, REP_MEMBERS)
                : MemoryPropertyBuilder.create(WEAKREFERENCE, property);
            if (propertyBuilder.hasValue(toRemove)) {
                propertyBuilder.removeValue(toRemove);
                if (propertyBuilder.isEmpty()) {
                    groupTree.removeProperty(REP_MEMBERS);
                } else {
                    groupTree.setProperty(propertyBuilder.getPropertyState(true));
                }
                return true;
            }
        }

        // nothing changed
        log.debug("Authorizable {} was not member of {}", memberTree.getName(), groupTree.getName());
        return false;
    }

    //-----------------------------------------< private MembershipProvider >---

    private boolean useMemberNode(Tree groupTree) {
        return splitSize >= 4 && !groupTree.hasProperty(REP_MEMBERS);
    }

    /**
     * Returns an iterator of authorizables which includes all indirect members
     * of the given iterator of authorizables.
     *
     *
     * @param declaredMembers Iterator containing the paths to the declared members.
     * @param authorizableType Flag used to filter the result by authorizable type.
     * @return Iterator of Authorizable objects
     */
    private Iterator<String> getAllMembers(final Iterator<String> declaredMembers,
                                           final AuthorizableType authorizableType) {
        Iterator<Iterator<String>> inheritedMembers = new Iterator<Iterator<String>>() {
            @Override
            public boolean hasNext() {
                return declaredMembers.hasNext();
            }

            @Override
            public Iterator<String> next() {
                String memberPath = declaredMembers.next();
                if (memberPath == null) {
                    return Iterators.emptyIterator();
                } else {
                    return Iterators.concat(Iterators.singletonIterator(memberPath), inherited(memberPath));
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private Iterator<String> inherited(String authorizablePath) {
                Tree group = getByPath(authorizablePath);
                if (UserUtility.isType(group, AuthorizableType.GROUP)) {
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
                if (UserUtility.isType(group, AuthorizableType.GROUP)) {
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