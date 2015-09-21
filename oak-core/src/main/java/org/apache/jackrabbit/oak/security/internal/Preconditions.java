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
package org.apache.jackrabbit.oak.security.internal;

import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;

/**
 * Represents a preconditions set that may be satisfied by adding the right
 * candidates.
 * <p/>
 * Initially, a set of preconditions is empty. An empty set of preconditions is
 * always satisfied. If candidates are added, but the precondition set is empty,
 * the preconditions are considered satisfied.
 * <p/>
 * When some preconditions are added, the preconditions set may enter into the
 * unsatisfied state. In this case, the preconditions set may be come satisfied
 * again only with the addition of the right candidates.
 * <p/>
 * This class doesn't admit duplicates for preconditions or candidates. Adding
 * the same precondition (or candidate) twice doesn't have any effect on the
 * state of the preconditions set.
 */
class Preconditions {

    private final Set<String> preconditions = newHashSet();

    private final Set<String> candidates = newHashSet();

    private boolean dirty = false;

    private boolean satisfied = true;

    /**
     * Add a precondition to this preconditions set. If the precondition already
     * belongs to this set, this operation has no effect.
     *
     * @param precondition The precondition to be added.
     */
    public void addPrecondition(String precondition) {
        if (preconditions.add(precondition)) {
            dirty = true;
        }
    }

    /**
     * Remove all the preconditions to this set. This makes the set of
     * preconditions empty and, as such, satisfied.
     */
    public void clearPreconditions() {
        preconditions.clear();
        dirty = false;
        satisfied = true;
    }

    /**
     * Add a candidate to this preconditions set. If the candidate already
     * belongs to this set, this operation has no effect.
     *
     * @param candidate The candidate to be added.
     */
    public void addCandidate(String candidate) {
        if (candidates.add(candidate)) {
            dirty = true;
        }
    }

    /**
     * Remove a candidate from this preconditions set. If the candidate doesn't
     * belong to this set, this operation has no effect.
     *
     * @param candidate The candidate to be removed.
     */
    public void removeCandidate(String candidate) {
        if (candidates.remove(candidate)) {
            dirty = true;
        }
    }

    /**
     * Check if the preconditions set are satisfied.
     *
     * @return {@code true} if the preconditions set is satisfied, {@code false}
     * otherwise.
     */
    public boolean areSatisfied() {
        if (dirty) {
            satisfied = candidates.containsAll(preconditions);
            dirty = false;
        }

        return satisfied;
    }

    @Override
    public String toString() {
        return String.format("Preconditions(preconditions = %s, candidates = %s)", preconditions, candidates);
    }

}
