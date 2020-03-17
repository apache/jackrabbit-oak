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

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PreconditionsTest {

    private Preconditions preconditions = new Preconditions();

    @Test
    public void testEmptyIsSatisfied() {
        assertTrue(preconditions.areSatisfied());
    }

    @Test
    public void testAddPrecondition() {
        preconditions.addPrecondition("a");
        assertFalse(preconditions.areSatisfied());
    }

    @Test
    public void testClearPrecondition() {
        preconditions.addPrecondition("a");
        preconditions.clearPreconditions();
        assertTrue(preconditions.areSatisfied());
    }

    @Test
    public void testAddNonMatchingCandidate() {
        preconditions.addPrecondition("a");
        preconditions.addCandidate("c");
        assertFalse(preconditions.areSatisfied());
    }

    @Test
    public void testAddCandidateNoPrecondition() {
        preconditions.addCandidate("a");
        assertTrue(preconditions.areSatisfied());
    }

    @Test
    public void testAddMatchingCandidate() {
        preconditions.addPrecondition("a");
        preconditions.addCandidate("a");
        assertTrue(preconditions.areSatisfied());
    }

    @Test
    public void testRemoveMatchingCandidate() {
        preconditions.addPrecondition("a");
        preconditions.addCandidate("a");
        preconditions.removeCandidate("a");
        assertFalse(preconditions.areSatisfied());
    }
}