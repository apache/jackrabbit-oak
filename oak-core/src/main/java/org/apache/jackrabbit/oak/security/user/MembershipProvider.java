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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.commons.iterator.AbstractLazyIterator;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code MembershipProvider} implementation storing group membership information
 * with the {@code Tree} associated with a given {@link org.apache.jackrabbit.api.security.user.Group}.
 *
 * As of Oak the {@code MembershipProvider} automatically chooses an appropriate storage structure
 * depending on the number of group members. If the number of members is low they are stored as
 * {@link javax.jcr.PropertyType#WEAKREFERENCE} in the {@link #REP_MEMBERS} multi value property. This is similar to
 * Jackrabbit 2.x.
 *
 * If the number of members is high the {@code MembershipProvider} will create an intermediate node list to reduce the
 * size of the multi value properties below a {@link #REP_MEMBERS_LIST} node. The provider will maintain a number of
 * sub nodes of type {@link #NT_REP_MEMBER_REFERENCES} that again store the member references in a {@link #REP_MEMBERS}
 * property.
 *
 * Note that the writing of the members is done in {@link MembershipWriter} so that the logic can be re-used by the
 * migration code.
 *
 * The current implementation uses a fixed threshold value of {@link #getMembershipSizeThreshold()} before creating
 * {@link #NT_REP_MEMBER_REFERENCES} sub nodes.
 *
 * Example Group with few members (irrelevant properties excluded):
 * <xmp>
     {
         "jcr:primaryType": "rep:Group",
         "rep:principalName": "contributor",
         "rep:members": [
             "429bbd5b-46a6-3c3d-808b-5fd4219d5c4d",
             "ca58c408-fe06-357e-953c-2d23ffe1e096",
             "3ebb1c04-76dd-317e-a9ee-5164182bc390",
             "d3c827d3-4db2-30cc-9c41-0ed8117dbaff",
             "f5777a0b-a933-3b4d-9405-613d8bc39cc7",
             "fdd1547a-b19a-3154-90da-1eae8c2c3504",
             "65c3084e-abfc-3719-8223-72c6cb9a3d6f"
         ]
     }
 * </xmp>
 *
 * Example Group with many members (irrelevant properties excluded):
 * <xmp>
     {
         "jcr:primaryType": "rep:Group",
         "rep:principalName": "employees",
         "rep:membersList": {
             "jcr:primaryType": "rep:MemberReferencesList",
             "0": {
                 "jcr:primaryType": "rep:MemberReferences",
                 "rep:members": [
                     "429bbd5b-46a6-3c3d-808b-5fd4219d5c4d",
                     "ca58c408-fe06-357e-953c-2d23ffe1e096",
                     ...
                 ]
             },
             ...
             "341": {
                 "jcr:primaryType": "rep:MemberReferences",
                 "rep:members": [
                     "fdd1547a-b19a-3154-90da-1eae8c2c3504",
                     "65c3084e-abfc-3719-8223-72c6cb9a3d6f",
                     ...
                 ]
             }
         }
     }
 * </xmp>
 */
class MembershipProvider extends AuthorizableBaseProvider {

    private static final Logger log = LoggerFactory.getLogger(MembershipProvider.class);

    private final MembershipWriter writer = new MembershipWriter();

    /**
     * Creates a new membership provider
     * @param root the current root
     * @param config the security configuration
     */
    MembershipProvider(Root root, ConfigurationParameters config) {
        super(root, config);
    }

    /**
     * Returns the size of the membership property threshold. This is currently only useful for testing.
     * @return the size of the membership property threshold.
     */
    int getMembershipSizeThreshold() {
        return writer.getMembershipSizeThreshold();
    }

    /**
     * Sets the size of the membership property threshold. This is currently only useful for testing.
     * @param membershipSizeThreshold the size of the membership property threshold
     */
    void setMembershipSizeThreshold(int membershipSizeThreshold) {
        writer.setMembershipSizeThreshold(membershipSizeThreshold);
    }

    /**
     * Returns an iterator over all membership paths of the given authorizable.
     *
     * @param authorizableTree the authorizable tree
     * @param includeInherited {@code true} to include inherited memberships
     * @return an iterator over all membership paths.
     */
    @Nonnull
    Iterator<String> getMembership(Tree authorizableTree, final boolean includeInherited) {
        return getMembership(authorizableTree, includeInherited, new HashSet<String>());
    }

    /**
     * Returns an iterator over all membership paths of the given authorizable.
     *
     * @param authorizableTree the authorizable tree
     * @param includeInherited {@code true} to include inherited memberships
     * @param processedPaths helper set that contains the processed paths
     * @return an iterator over all membership paths.
     */
    @Nonnull
    private Iterator<String> getMembership(Tree authorizableTree, final boolean includeInherited,
                                           final Set<String> processedPaths) {
        final Iterable<String> refPaths = identifierManager.getReferences(
                true, authorizableTree, REP_MEMBERS, NT_REP_MEMBER_REFERENCES
        );

        return new AbstractLazyIterator<String>() {

            private final Iterator<String> references = refPaths.iterator();

            private Iterator<String> parent;

            @Override
            protected String getNext() {
                String next = null;
                while (next == null) {
                    if (parent != null) {
                        // if we have a parent iterator, process it first
                        if (parent.hasNext()) {
                            next = parent.next();
                        } else {
                            parent = null;
                        }
                    } else if (!references.hasNext()) {
                        // if not, check if we have more references to process and abort if not
                        break;
                    } else {
                        // get the next rep:members property path
                        String propPath = references.next();
                        int index = propPath.indexOf('/' + REP_MEMBERS_LIST);
                        if (index < 0) {
                            index = propPath.indexOf('/' + REP_MEMBERS);
                        }
                        if (index > 0) {
                            String groupPath = propPath.substring(0, index);
                            if (processedPaths.add(groupPath)) {
                                // we didn't see this path before, so continue
                                next = groupPath;
                                if (includeInherited) {
                                    // inject a parent iterator of the inherited memberships is needed
                                    Tree group = getByPath(groupPath);
                                    if (UserUtil.isType(group, AuthorizableType.GROUP)) {
                                        parent = getMembership(group, true, processedPaths);
                                    }
                                }
                            }
                        } else {
                            log.debug("Not a membership reference property " + propPath);
                        }
                    }
                }
                return next;
            }
        };
    }

    /**
     * Returns an iterator over all member paths of the given group.
     *
     * @param groupTree the group tree
     * @param authorizableType type of authorizables to filter.
     * @param includeInherited {@code true} to include inherited members
     * @return an iterator over all member paths
     */
    @Nonnull
    Iterator<String> getMembers(@Nonnull Tree groupTree, @Nonnull AuthorizableType authorizableType, boolean includeInherited) {
        return getMembers(groupTree, authorizableType, includeInherited, new HashSet<String>());
    }

    /**
     * Returns an iterator over all member paths of the given group.
     *
     * @param groupTree the group tree
     * @param authorizableType type of authorizables to filter.
     * @param includeInherited {@code true} to include inherited members
     * @param processedRefs helper set that contains the references that are already processed.
     * @return an iterator over all member paths
     */
    @Nonnull
    private Iterator<String> getMembers(@Nonnull final Tree groupTree, @Nonnull final AuthorizableType authorizableType,
                                final boolean includeInherited, @Nonnull final Set<String> processedRefs) {

        return new AbstractLazyIterator<String>() {

            private MemberReferenceIterator references = new MemberReferenceIterator(groupTree, processedRefs);

            private Iterator<String> parent;

            @Override
            protected String getNext() {
                String next = null;
                while (next == null) {
                    // process parent iterators first
                    if (parent != null) {
                        if (parent.hasNext()) {
                            next = parent.next();
                        } else {
                            parent = null;
                        }
                    } else if (!references.hasNext()) {
                        // if there are no more values left, reset the iterator
                        break;
                    } else {
                        String value = references.next();
                        next = identifierManager.getPath(PropertyValues.newWeakReference(value));

                        // filter by authorizable type, and/or get inherited members
                        if (next != null && (includeInherited || authorizableType != AuthorizableType.AUTHORIZABLE)) {
                            Tree auth = getByPath(next);
                            AuthorizableType type = UserUtil.getType(auth);

                            if (includeInherited && type == AuthorizableType.GROUP) {
                                parent = getMembers(auth, authorizableType, true, processedRefs);
                            }
                            if (authorizableType != AuthorizableType.AUTHORIZABLE && type != authorizableType) {
                                next = null;
                            }
                        }
                    }
                }
                return next;
            }
        };
    }

    /**
     * Returns {@code true} if the given {@code groupTree} contains a member with the given {@code authorizableTree}
     *
     * @param groupTree  The new member to be tested for cyclic membership.
     * @param authorizableTree The authorizable to check
     * @param includeInherited {@code true} to also check inherited members
     *
     * @return true if the group has given member.
     */
    boolean isMember(Tree groupTree, Tree authorizableTree, boolean includeInherited) {
        return isMember(groupTree, getContentID(authorizableTree), includeInherited);
    }

    /**
     * Returns {@code true} if the given {@code groupTree} contains a member with the given {@code contentId}
     *
     * @param groupTree  The new member to be tested for cyclic membership.
     * @param contentId The content ID of the group.
     * @param includeInherited {@code true} to also check inherited members
     *
     * @return true if the group has given member.
     */
    boolean isMember(Tree groupTree, String contentId, boolean includeInherited) {
        if (includeInherited) {
            Set<String> refs = new HashSet<String>();
            for (Iterator<String> it = getMembers(groupTree, AuthorizableType.AUTHORIZABLE, includeInherited, refs); it.hasNext();) {
                it.next();
                if (refs.contains(contentId)) {
                    return true;
                }
            }
        } else {
            MemberReferenceIterator refs = new MemberReferenceIterator(groupTree, new HashSet<String>());
            while (refs.hasNext()) {
                if (contentId.equals(refs.next())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Adds a new member to the given {@code groupTree}.
     * @param groupTree the group to add the member to
     * @param newMemberTree the tree of the new member
     * @return {@code true} if the member was added
     * @throws RepositoryException if an error occurs
     */
    boolean addMember(Tree groupTree, Tree newMemberTree) throws RepositoryException {
        return writer.addMember(groupTree, getContentID(newMemberTree));
    }

    /**
     * Adds a new member to the given {@code groupTree}.
     * @param groupTree the group to add the member to
     * @param memberContentId the id of the new member
     * @return {@code true} if the member was added
     * @throws RepositoryException if an error occurs
     */
    boolean addMember(Tree groupTree, String memberContentId) throws RepositoryException {
        return writer.addMember(groupTree, memberContentId);
    }

    /**
     * Removes the member from the given group.
     *
     * @param groupTree group to remove the member from
     * @param memberTree member to remove
     * @return {@code true} if the member was removed.
     */
    boolean removeMember(Tree groupTree, Tree memberTree) {
        if (writer.removeMember(groupTree, getContentID(memberTree))) {
            return true;
        } else {
            log.debug("Authorizable {} was not member of {}", memberTree.getName(), groupTree.getName());
            return false;
        }
    }

    /**
     * Iterator that provides member references based on the rep:members properties of a underlying tree iterator.
     */
    private class MemberReferenceIterator extends AbstractLazyIterator<String> {

        private final Set<String> processedRefs;

        private final Iterator<Tree> trees;

        private Iterator<String> propertyValues;

        private MemberReferenceIterator(@Nonnull Tree groupTree, @Nonnull Set<String> processedRefs) {
            this.processedRefs = processedRefs;
            this.trees = Iterators.concat(
                    Iterators.singletonIterator(groupTree),
                    groupTree.getChild(REP_MEMBERS_LIST).getChildren().iterator()
            );
        }

        @Override
        protected String getNext() {
            String next = null;
            while (next == null) {

                if (propertyValues == null) {
                    // check if there are more trees that can provide a rep:members property
                    if (!trees.hasNext()) {
                        // if not, we're done
                        break;
                    }
                    PropertyState property = trees.next().getProperty(REP_MEMBERS);
                    if (property != null) {
                        propertyValues = property.getValue(Type.STRINGS).iterator();
                    }
                } else if (!propertyValues.hasNext()) {
                    // if there are no more values left, reset the iterator
                    propertyValues = null;
                } else {
                    String value = propertyValues.next();
                    if (processedRefs.add(value)) {
                        next = value;
                    }
                }
            }
            return next;
        }
    }
}