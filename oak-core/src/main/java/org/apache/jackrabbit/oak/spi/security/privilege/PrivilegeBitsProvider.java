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
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
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
    private final Map<String, Set<String>> aggregation = new HashMap<String, Set<String>>();

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
     * Returns the bits for the given privilege names
     * @param privilegeNames the names
     * @return the privilege bits
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
     * Returns the bits for the given privilege names
     * @param privilegeNames the names
     * @return the privilege bits
     */
    @Nonnull
    public PrivilegeBits getBits(@Nonnull Iterable<String> privilegeNames) {
        if (Iterables.isEmpty(privilegeNames)) {
            return PrivilegeBits.EMPTY;
        }

        Tree privilegesTree = null;
        PrivilegeBits bits = PrivilegeBits.getInstance();
        for (String privilegeName : privilegeNames) {
            PrivilegeBits builtIn = PrivilegeBits.BUILT_IN.get(privilegeName);
            if (builtIn != null) {
                bits.add(builtIn);
            } else {
                if (privilegesTree == null) {
                    privilegesTree = getPrivilegesTree();
                }
                if (privilegesTree.exists() && privilegesTree.hasChild(privilegeName)) {
                    Tree defTree = privilegesTree.getChild(privilegeName);
                    bits.add(PrivilegeBits.getInstance(defTree));
                } else {
                    log.debug("Ignoring privilege name " + privilegeName);
                }
            }
        }
        return bits.unmodifiable();
    }

    /**
     * Returns the bits for the given privileges
     *
     * @param privileges the privileges
     * @param nameMapper the name mapper
     * @return the privilege bits
     */
    @Nonnull
    public PrivilegeBits getBits(@Nonnull Privilege[] privileges, @Nonnull final NameMapper nameMapper) {
        return getBits(Iterables.filter(Iterables.transform(Arrays.asList(privileges), new Function<Privilege, String>() {

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
        }), Predicates.notNull()));
    }

    /**
     * Resolve the given privilege bits to a set of privilege names.
     *
     * @param privilegeBits An instance of privilege bits.
     * @return The names of the registered privileges associated with the given
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

    /**
     * Return the names of the non-aggregate privileges corresponding to the
     * specified {@code privilegeNames}.
     *
     * @param privilegeNames The privilege names to be converted.
     * @return The names of the non-aggregate privileges that correspond to the
     * given {@code privilegeNames}.
     */
    @Nonnull
    public Iterable<String> getAggregatedPrivilegeNames(@Nonnull String... privilegeNames) {
        if (privilegeNames.length == 0) {
            return Collections.emptySet();
        } else if (privilegeNames.length == 1) {
            String privName = privilegeNames[0];
            if (NON_AGGREGATE_PRIVILEGES.contains(privName)) {
                return ImmutableSet.of(privName);
            } else if (aggregation.containsKey(privName)) {
                return aggregation.get(privName);
            } else if (AGGREGATE_PRIVILEGES.keySet().contains(privName)) {
                Set<String> aggregates = resolveBuiltInAggregation(privName);
                aggregation.put(privName, aggregates);
                return aggregates;
            } else {
                return extractAggregatedPrivileges(Collections.singleton(privName));
            }
        } else {
            Set<String> pNames = ImmutableSet.copyOf(privilegeNames);
            if (NON_AGGREGATE_PRIVILEGES.containsAll(pNames)) {
                return pNames;
            } else {
                return extractAggregatedPrivileges(pNames);
            }
        }
    }

    private Iterable<String> extractAggregatedPrivileges(@Nonnull Iterable<String> privilegeNames) {
        return FluentIterable.from(privilegeNames).transformAndConcat(new ExtractAggregatedPrivileges());
    }

    private Set<String> resolveBuiltInAggregation(@Nonnull String privilegeName) {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (String name : AGGREGATE_PRIVILEGES.get(privilegeName)) {
            if (!AGGREGATE_PRIVILEGES.containsKey(name)) {
                builder.add(name);
            } else {
                builder.addAll(resolveBuiltInAggregation(name));
            }
        }
        Set<String> set = builder.build();
        aggregation.put(privilegeName, set);
        return set;
    }

    private final class ExtractAggregatedPrivileges implements Function<String, Iterable<String>> {
        @Nonnull
        @Override
        public Iterable<String> apply(@Nullable String privName) {
            if (privName == null) {
                return Collections.emptySet();
            } else {
                if (NON_AGGREGATE_PRIVILEGES.contains(privName)) {
                    return Collections.singleton(privName);
                } else if (aggregation.containsKey(privName)) {
                    return aggregation.get(privName);
                } else if (AGGREGATE_PRIVILEGES.containsKey(privName)) {
                    return resolveBuiltInAggregation(privName);
                } else {
                    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
                    fillAggregation(getPrivilegesTree().getChild(privName), builder);

                    Set<String> aggregates = builder.build();
                    if (!JCR_ALL.equals(privName) && !aggregates.isEmpty()) {
                        aggregation.put(privName, aggregates);
                    }
                    return aggregates;
                }
            }
        }

        private void fillAggregation(@Nonnull Tree privTree, @Nonnull ImmutableSet.Builder<String> builder) {
            if (!privTree.exists()) {
                return;
            }
            PropertyState aggregates = privTree.getProperty(REP_AGGREGATES);
            if (aggregates != null) {
                for (String name : aggregates.getValue(Type.NAMES)) {
                    if (NON_AGGREGATE_PRIVILEGES.contains(name)) {
                        builder.add(name);
                    } else if (aggregation.containsKey(name)) {
                        builder.addAll(aggregation.get(name));
                    } else if (AGGREGATE_PRIVILEGES.containsKey(name)) {
                        builder.addAll(resolveBuiltInAggregation(name));
                    } else {
                        fillAggregation(privTree.getParent().getChild(name), builder);
                    }
                }
            } else {
                builder.add(privTree.getName());
            }
        }
    }
}
