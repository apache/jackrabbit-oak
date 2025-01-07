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
import java.util.function.Function;

import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.guava.common.collect.FluentIterable;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows to obtain the internal {@link PrivilegeBits representation} of privileges (or their names) and to covert the 
 * internal representation back to privilege names.
 */
public final class PrivilegeBitsProvider implements PrivilegeConstants {

    private static final Logger log = LoggerFactory.getLogger(PrivilegeBitsProvider.class);

    private final Map<PrivilegeBits, Set<String>> bitsToNames = new HashMap<>();
    private final Map<String, PrivilegeBits> nameToBits = new HashMap<>();
    private final Map<String, Set<String>> aggregation = new HashMap<>();

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
    @NotNull
    public Tree getPrivilegesTree() {
        return PrivilegeUtil.getPrivilegesTree(root);
    }

    /**
     * Returns the bits for the given privilege names.
     * 
     * @param privilegeNames the names
     * @return the privilege bits representing the given privilege names.
     */
    @NotNull
    public PrivilegeBits getBits(@NotNull String... privilegeNames) {
        if (privilegeNames.length == 0) {
            return PrivilegeBits.EMPTY;
        } else {
            return getBits(Arrays.asList(privilegeNames));
        }
    }

    /**
     * Returns the bits for the given privilege names. Note, that any invalid privilege names will be ignored.
     * 
     * @param privilegeNames the names
     * @return the privilege bits representing the given privilege names.
     */
    @NotNull
    public PrivilegeBits getBits(@NotNull Iterable<String> privilegeNames) {
        if (Iterables.isEmpty(privilegeNames)) {
            return PrivilegeBits.EMPTY;
        }

        PrivilegeBits bits = PrivilegeBits.getInstance();
        collectBits(privilegeNames, bits);
        return bits.unmodifiable();
    }

    /**
     * Returns the bits for the given privilege names with the option to verify that all privilege names point to a valid,
     * registered privilege.
     * 
     * @param privilegeNames An iterable of privilege names.
     * @param validateNames If set to {@code true} this method will throw an AccessControlException if an invalid privilege 
     * name is found (i.e. one that doesn't represent a registered privilege). If set to {@code false} invalid privilege 
     * names will be ignored i.e. making this method equivalent to {@link #getBits(String...)}.
     * @return the privilege bits representing the given privilege names.
     * @throws AccessControlException If {@code validateNames} is {@code true} and the any of the specified privilege names is invalid.
     */
    @NotNull
    public PrivilegeBits getBits(@NotNull Iterable<String> privilegeNames, boolean validateNames) throws AccessControlException {
        if (!validateNames) {
            return getBits(privilegeNames);
        }
        if (Iterables.isEmpty(privilegeNames)) {
            return PrivilegeBits.EMPTY;
        }
        PrivilegeBits bits = PrivilegeBits.getInstance();
        if (!collectBits(privilegeNames, bits)) {
            throw new AccessControlException("Invalid privilege name contained in " + privilegeNames);
        }
        return bits.unmodifiable();
    }
    
    private boolean collectBits(@NotNull Iterable<String> privilegeNames, @NotNull PrivilegeBits bits) {
        Tree privilegesTree = null;
        boolean allNamesValid = true;
        for (String privilegeName : privilegeNames) {
            PrivilegeBits builtIn = PrivilegeBits.BUILT_IN.get(privilegeName);
            if (builtIn != null) {
                bits.add(builtIn);
            } else if (nameToBits.containsKey(privilegeName)) {
                bits.add(nameToBits.get(privilegeName));
            }  else {
                if (privilegesTree == null) {
                    privilegesTree = getPrivilegesTree();
                }
                if (privilegesTree.exists() && privilegesTree.hasChild(privilegeName)) {
                    Tree defTree = privilegesTree.getChild(privilegeName);
                    PrivilegeBits bitsFromDefTree = PrivilegeBits.getInstance(defTree);
                    nameToBits.put(privilegeName, bitsFromDefTree);
                    bits.add(bitsFromDefTree);
                } else {
                    log.debug("Invalid privilege name {}", privilegeName);
                    allNamesValid = false;
                }
            }
        }
        return allNamesValid;
    }

    /**
     * Returns the bits for the given array of privileges.
     *
     * @param privileges An array of privileges
     * @param nameMapper the name mapper
     * @return the privilege bits representing the given array of privileges.
     */
    @NotNull
    public PrivilegeBits getBits(@NotNull Privilege[] privileges, @NotNull final NameMapper nameMapper) {
        return getBits(Iterables.filter(Iterables.transform(Arrays.asList(privileges),
                privilege -> nameMapper.getOakNameOrNull(privilege.getName())), x -> x != null));
    }

    /**
     * Resolve the given privilege bits to the corresponding set of privilege names.
     *
     * @param privilegeBits An instance of privilege bits.
     * @return The names of the registered privileges associated with the given
     *         bits. Any bits that don't have a corresponding privilege definition will
     *         be ignored.
     */
    @NotNull
    public Set<String> getPrivilegeNames(@Nullable PrivilegeBits privilegeBits) {
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
                privilegeNames = collectPrivilegeNames(privilegesTree, pb);
                bitsToNames.put(pb, Set.copyOf(privilegeNames));
            }
            return privilegeNames;
        }
    }

    @NotNull
    private static Set<String> collectPrivilegeNames(@NotNull Tree privilegesTree, @NotNull PrivilegeBits pb) {
        Set<String> privilegeNames = new HashSet<>();
        Set<String> aggregates = new HashSet<>();
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
        return privilegeNames;
    }

    /**
     * Return the names of the non-aggregate privilege names corresponding to the
     * specified {@code privilegeNames}.
     *
     * @param privilegeNames The privilege names to be converted.
     * @return The names of the non-aggregate privileges that correspond to the
     * given {@code privilegeNames}.
     */
    @NotNull
    public Iterable<String> getAggregatedPrivilegeNames(@NotNull String... privilegeNames) {
        if (privilegeNames.length == 0) {
            return Collections.emptySet();
        } else if (privilegeNames.length == 1) {
            String privName = privilegeNames[0];
            if (NON_AGGREGATE_PRIVILEGES.contains(privName)) {
                return Set.of(privName);
            } else if (aggregation.containsKey(privName)) {
                return aggregation.get(privName);
            } else if (AGGREGATE_PRIVILEGES.containsKey(privName)) {
                Set<String> aggregates = resolveBuiltInAggregation(privName);
                aggregation.put(privName, aggregates);
                return aggregates;
            } else {
                return extractAggregatedPrivileges(Collections.singleton(privName));
            }
        } else {
            Set<String> pNames = Set.of(privilegeNames);
            if (NON_AGGREGATE_PRIVILEGES.containsAll(pNames)) {
                return pNames;
            } else {
                return extractAggregatedPrivileges(pNames);
            }
        }
    }

    @NotNull
    private Iterable<String> extractAggregatedPrivileges(@NotNull Iterable<String> privilegeNames) {
        return FluentIterable.from(privilegeNames).transformAndConcat(new ExtractAggregatedPrivileges()::apply);
    }

    @NotNull
    private Set<String> resolveBuiltInAggregation(@NotNull String privilegeName) {
        Set<String> set = new HashSet<>();
        for (String name : AGGREGATE_PRIVILEGES.get(privilegeName)) {
            if (!AGGREGATE_PRIVILEGES.containsKey(name)) {
                set.add(name);
            } else {
                set.addAll(resolveBuiltInAggregation(name));
            }
        }
        aggregation.put(privilegeName, set);
        return set;
    }

    private final class ExtractAggregatedPrivileges implements Function<String, Iterable<String>> {
        @NotNull
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
                    Set<String> aggregates = new HashSet<>();
                    fillAggregation(getPrivilegesTree().getChild(privName), aggregates);

                    if (!JCR_ALL.equals(privName) && !aggregates.isEmpty()) {
                        aggregation.put(privName, aggregates);
                    }
                    return Set.copyOf(aggregates);
                }
            }
        }

        private void fillAggregation(@NotNull Tree privTree, @NotNull Set<String> set) {
            if (!privTree.exists()) {
                return;
            }
            PropertyState aggregates = privTree.getProperty(REP_AGGREGATES);
            if (aggregates != null) {
                for (String name : aggregates.getValue(Type.NAMES)) {
                    if (NON_AGGREGATE_PRIVILEGES.contains(name)) {
                        set.add(name);
                    } else if (aggregation.containsKey(name)) {
                        set.addAll(aggregation.get(name));
                    } else if (AGGREGATE_PRIVILEGES.containsKey(name)) {
                        set.addAll(resolveBuiltInAggregation(name));
                    } else {
                        fillAggregation(privTree.getParent().getChild(name), set);
                    }
                }
            } else {
                set.add(privTree.getName());
            }
        }
    }
}
