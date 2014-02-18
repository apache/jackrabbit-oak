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
package org.apache.jackrabbit.oak.spi.security.privilege;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.security.Privilege;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads and writes privilege definitions from and to the repository content
 * without applying any validation.
 */
public final class PrivilegeBitsProvider implements PrivilegeConstants {

    private static final Logger log = LoggerFactory.getLogger(PrivilegeBitsProvider.class);

    private final Map<PrivilegeBits, Set<String>> bitsToNames = new HashMap<PrivilegeBits, Set<String>>();

    private final Root root;

    public PrivilegeBitsProvider(Root root) {
        this.root = root;
    }

    /**
     * Returns the root tree for all privilege definitions stored in the content
     * repository.
     *
     * @return The privileges root.
     */
    @Nonnull
    public Tree getPrivilegesTree() {
        return PrivilegeUtil.getPrivilegesTree(root);
    }

    /**
     * @param privilegeNames
     * @return
     */
    @Nonnull
    public PrivilegeBits getBits(@Nonnull String... privilegeNames) {
        if (privilegeNames.length == 0) {
            return PrivilegeBits.EMPTY;
        } else {
            return getBits(Arrays.asList(privilegeNames));
        }
    }

    /**
     * @param privilegeNames
     * @return
     */
    @Nonnull
    public PrivilegeBits getBits(@Nonnull Iterable<String> privilegeNames) {
        if (!privilegeNames.iterator().hasNext()) {
            return PrivilegeBits.EMPTY;
        }

        Tree privilegesTree = getPrivilegesTree();
        if (!privilegesTree.exists()) {
            return PrivilegeBits.EMPTY;
        }
        PrivilegeBits bits = PrivilegeBits.getInstance();
        for (String privilegeName : privilegeNames) {
            if (privilegesTree.hasChild(privilegeName)) {
                Tree defTree = privilegesTree.getChild(privilegeName);
                bits.add(PrivilegeBits.getInstance(defTree));
            } else {
                log.debug("Ignoring privilege name " + privilegeName);
            }
        }
        return bits.unmodifiable();
    }

    /**
     *
     * @param privileges
     * @param nameMapper
     * @return
     */
    @Nonnull
    public PrivilegeBits getBits(@Nonnull Privilege[] privileges, final @Nonnull NameMapper nameMapper) {
        return getBits(Iterables.transform(Arrays.asList(privileges), new Function<Privilege, String>() {

            @Override
            public String apply(@Nullable Privilege privilege) {
                if (privilege != null) {
                    try {
                        return nameMapper.getOakName(privilege.getName());
                    } catch (RepositoryException e) {
                        log.debug("Unable to resolve OAK name of privilege " + privilege, e);
                    }
                }
                // null privilege or failed to resolve the privilege name
                return null;
            }
        }));
    }

    /**
     * Resolve the given privilege bits to a set of privilege names.
     *
     * @param privilegeBits An instance of privilege bits.
     * @return The names of the registed privileges associated with the given
     *         bits. Any bits that don't have a corresponding privilege definition will
     *         be ignored.
     */
    @Nonnull
    public Set<String> getPrivilegeNames(PrivilegeBits privilegeBits) {
        if (privilegeBits == null || privilegeBits.isEmpty()) {
            return Collections.emptySet();
        }

        PrivilegeBits pb = privilegeBits.unmodifiable();
        if (bitsToNames.containsKey(pb)) {
            // matches all built-in aggregates and single built-in privileges
            return bitsToNames.get(pb);
        } else {
            Tree privilegesTree = getPrivilegesTree();
            if (!privilegesTree.exists()) {
                return Collections.emptySet();
            }

            if (bitsToNames.isEmpty()) {
                for (Tree child : privilegesTree.getChildren()) {
                    bitsToNames.put(PrivilegeBits.getInstance(child), Collections.singleton(child.getName()));
                }
            }

            Set<String> privilegeNames;
            if (bitsToNames.containsKey(pb)) {
                privilegeNames = bitsToNames.get(pb);
            } else {
                privilegeNames = new HashSet<String>();
                Set<String> aggregates = new HashSet<String>();
                for (Tree child : privilegesTree.getChildren()) {
                    PrivilegeBits bits = PrivilegeBits.getInstance(child);
                    if (pb.includes(bits)) {
                        privilegeNames.add(child.getName());
                        if (child.hasProperty(REP_AGGREGATES)) {
                            aggregates.addAll(PrivilegeUtil.readDefinition(child).getDeclaredAggregateNames());
                        }
                    }
                }
                privilegeNames.removeAll(aggregates);
                bitsToNames.put(pb, ImmutableSet.copyOf(privilegeNames));
            }
            return privilegeNames;
        }
    }
}
