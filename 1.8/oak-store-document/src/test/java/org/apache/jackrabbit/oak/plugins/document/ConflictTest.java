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
package org.apache.jackrabbit.oak.plugins.document;

import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * <code>ConflictTest</code> checks
 * <a href="http://wiki.apache.org/jackrabbit/Conflict%20handling%20through%20rebasing%20branches">conflict handling</a>.
 */
public class ConflictTest extends BaseDocumentMKTest {

    @Test
    public void addExistingProperty() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        mk.commit("/foo", "^\"prop\":\"value\"", rev, null);

        try {
            mk.commit("/foo", "^\"prop\":\"value\"", rev, null);
            fail("Must fail with conflict for addExistingProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void addExistingPropertyBranchWins() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev = mk.branch(rev);
        branchRev = mk.commit("/foo", "^\"prop\":\"value\"", branchRev, null);
        mk.merge(branchRev, null);

        try {
            mk.commit("/foo", "^\"prop\":\"value\"", rev, null);
            fail("Must fail with conflict for addExistingProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void addExistingPropertyBranchLoses1() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev = mk.branch(rev);
        // branch commit happens before trunk commit
        branchRev = mk.commit("/foo", "^\"prop\":\"value\"", branchRev, null);
        mk.commit("/foo", "^\"prop\":\"value\"", rev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for addExistingProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void addExistingPropertyBranchLoses2() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev = mk.branch(rev);
        mk.commit("/foo", "^\"prop\":\"value\"", rev, null);
        // branch commit happens after trunk commit
        branchRev = mk.commit("/foo", "^\"prop\":\"value\"", branchRev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for addExistingProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void addExistingPropertyTwoBranches() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev1 = mk.branch(rev);
        branchRev1 = mk.commit("/foo", "^\"prop\":\"value\"", branchRev1, null);

        String branchRev2 = mk.branch(rev);
        // will mark conflict on branchRev1
        branchRev2 = mk.commit("/foo", "^\"prop\":\"other\"", branchRev2, null);

        mk.merge(branchRev2, null);

        try {
            mk.merge(branchRev1, null);
            fail("Must fail with conflict for addExistingProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeRemovedProperty() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        mk.commit("/foo", "^\"prop\":null", rev, null);

        try {
            mk.commit("/foo", "^\"prop\":null", rev, null);
            fail("Must fail with conflict for removeRemovedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeRemovedPropertyBranchWins() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        branchRev = mk.commit("/foo", "^\"prop\":null", branchRev, null);
        mk.merge(branchRev, null);

        try {
            mk.commit("/foo", "^\"prop\":null", rev, null);
            fail("Must fail with conflict for removeRemovedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeRemovedPropertyBranchLoses1() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        // branch commit happens before trunk commit
        branchRev = mk.commit("/foo", "^\"prop\":null", branchRev, null);
        mk.commit("/foo", "^\"prop\":null", rev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for removeRemovedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeRemovedPropertyBranchLoses2() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        mk.commit("/foo", "^\"prop\":null", rev, null);
        // branch commit happens after trunk commit
        branchRev = mk.commit("/foo", "^\"prop\":null", branchRev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for removeRemovedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeRemovedPropertyTwoBranches() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        String branchRev1 = mk.branch(rev);
        branchRev1 = mk.commit("/foo", "^\"prop\":null", branchRev1, null);

        String branchRev2 = mk.branch(rev);
        // will mark collision on branchRev1
        branchRev2 = mk.commit("/foo", "^\"prop\":null", branchRev2, null);

        mk.merge(branchRev2, null);

        try {
            mk.merge(branchRev1, null);
            fail("Must fail with conflict for removeRemovedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeChangedProperty() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        mk.commit("/foo", "^\"prop\":\"bar\"", rev, null);

        try {
            mk.commit("/foo", "^\"prop\":null", rev, null);
            fail("Must fail with conflict for removeChangedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeChangedPropertyBranchWins() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        branchRev = mk.commit("/foo", "^\"prop\":\"bar\"", branchRev, null);
        mk.merge(branchRev, null);

        try {
            mk.commit("/foo", "^\"prop\":null", rev, null);
            fail("Must fail with conflict for removeChangedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeChangedPropertyBranchLoses1() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        // branch commit happens before trunk commit
        branchRev = mk.commit("/foo", "^\"prop\":\"bar\"", branchRev, null);
        mk.commit("/foo", "^\"prop\":null", rev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for removeChangedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeChangedPropertyBranchLoses2() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        mk.commit("/foo", "^\"prop\":null", rev, null);
        // branch commit happens after trunk commit
        branchRev = mk.commit("/foo", "^\"prop\":\"bar\"", branchRev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for removeChangedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeChangedPropertyTwoBranches() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        String branchRev1 = mk.branch(rev);
        branchRev1 = mk.commit("/foo", "^\"prop\":null", branchRev1, null);

        String branchRev2 = mk.branch(rev);
        // will mark conflict on branchRev1
        branchRev2 = mk.commit("/foo", "^\"prop\":\"bar\"", branchRev2, null);
        mk.merge(branchRev2, null);

        try {
            mk.merge(branchRev1, null);
            fail("Must fail with conflict for removeChangedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void changeRemovedProperty() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        mk.commit("/foo", "^\"prop\":null", rev, null);

        try {
            mk.commit("/foo", "^\"prop\":\"bar\"", rev, null);
            fail("Must fail with conflict for changeRemovedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void changeRemovedPropertyBranchWins() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        branchRev = mk.commit("/foo", "^\"prop\":null", branchRev, null);
        mk.merge(branchRev, null);

        try {
            mk.commit("/foo", "^\"prop\":\"bar\"", rev, null);
            fail("Must fail with conflict for changeRemovedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void changeRemovedPropertyBranchLoses1() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        // branch commit happens before trunk commit
        branchRev = mk.commit("/foo", "^\"prop\":null", branchRev, null);
        mk.commit("/foo", "^\"prop\":\"bar\"", rev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for changeRemovedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void changeRemovedPropertyBranchLoses2() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        mk.commit("/foo", "^\"prop\":\"bar\"", rev, null);
        // branch commit happens after trunk commit
        branchRev = mk.commit("/foo", "^\"prop\":null", branchRev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for changeRemovedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void changeRemovedPropertyTwoBranches() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        String branchRev1 = mk.branch(rev);
        branchRev1 = mk.commit("/foo", "^\"prop\":\"bar\"", branchRev1, null);

        String branchRev2 = mk.branch(rev);
        // will mark collision on branchRev1
        branchRev2 = mk.commit("/foo", "^\"prop\":null", branchRev2, null);
        mk.merge(branchRev2, null);

        try {
            mk.merge(branchRev1, null);
            fail("Must fail with conflict for changeRemovedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void changeChangedProperty() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        mk.commit("/foo", "^\"prop\":\"bar\"", rev, null);

        try {
            mk.commit("/foo", "^\"prop\":\"baz\"", rev, null);
            fail("Must fail with conflict for changeChangedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void changeChangedPropertyBranchWins() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        branchRev = mk.commit("/foo", "^\"prop\":\"bar\"", branchRev, null);
        mk.merge(branchRev, null);

        try {
            mk.commit("/foo", "^\"prop\":\"baz\"", rev, null);
            fail("Must fail with conflict for changeChangedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void changeChangedPropertyBranchLoses1() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        // branch commit happens before trunk commit
        branchRev = mk.commit("/foo", "^\"prop\":\"bar\"", branchRev, null);
        mk.commit("/foo", "^\"prop\":\"baz\"", rev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for changeChangedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void changeChangedPropertyBranchLoses2() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        mk.commit("/foo", "^\"prop\":\"baz\"", rev, null);
        // branch commit happens after trunk commit
        branchRev = mk.commit("/foo", "^\"prop\":\"bar\"", branchRev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for changeChangedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void changeChangedPropertyTwoBranches() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);

        String b1 = mk.branch(rev);
        String b2 = mk.branch(rev);

        b1 = mk.commit("/foo", "^\"prop\":\"bar\"", b1, null);
        mk.merge(b1, null);

        b2 = mk.commit("/foo", "^\"prop\":\"baz\"", b2, null);
        try {
            mk.merge(b2, null);
            fail("Must fail with conflict for changeChangedProperty");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void addExistingNode() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        mk.commit("/foo", "+\"bar\":{}", rev, null);

        try {
            mk.commit("/foo", "+\"bar\":{}", rev, null);
            fail("Must fail with conflict for addExistingNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void addExistingNodeBranchWins() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev = mk.branch(rev);
        branchRev = mk.commit("/foo", "+\"bar\":{}", branchRev, null);
        mk.merge(branchRev, null);

        try {
            mk.commit("/foo", "+\"bar\":{}", rev, null);
            fail("Must fail with conflict for addExistingNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void addExistingNodeBranchLoses1() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev = mk.branch(rev);
        // branch commit happens before trunk commit
        branchRev = mk.commit("/foo", "+\"bar\":{}", branchRev, null);
        mk.commit("/foo", "+\"bar\":{}", rev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for addExistingNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void addExistingNodeBranchLoses2() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev = mk.branch(rev);
        mk.commit("/foo", "+\"bar\":{}", rev, null);
        // branch commit happens after trunk commit
        branchRev = mk.commit("/foo", "+\"bar\":{}", branchRev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for addExistingNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void addExistingNodeTwoBranches() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev1 = mk.branch(rev);
        branchRev1 = mk.commit("/foo", "+\"bar\":{}", branchRev1, null);

        String branchRev2 = mk.branch(rev);
        // will mark conflict on branchRev1
        branchRev2 = mk.commit("/foo", "+\"bar\":{}", branchRev2, null);

        mk.merge(branchRev2, null);

        try {
            mk.merge(branchRev1, null);
            fail("Must fail with conflict for addExistingNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeRemovedNode() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        mk.commit("/", "-\"foo\"", rev, null);

        try {
            mk.commit("/", "-\"foo\"", rev, null);
            fail("Must fail with conflict for removeRemovedNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeRemovedNodeBranchWins() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev = mk.branch(rev);
        branchRev = mk.commit("/", "-\"foo\"", branchRev, null);
        mk.merge(branchRev, null);

        try {
            mk.commit("/", "-\"foo\"", rev, null);
            fail("Must fail with conflict for removeRemovedNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeRemovedNodeBranchLoses1() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev = mk.branch(rev);
        // branch commit happens before trunk commit
        branchRev = mk.commit("/", "-\"foo\"", branchRev, null);
        mk.commit("/", "-\"foo\"", rev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for removeRemovedNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeRemovedNodeBranchLoses2() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev = mk.branch(rev);
        mk.commit("/", "-\"foo\"", rev, null);
        // branch commit happens after trunk commit
        branchRev = mk.commit("/", "-\"foo\"", branchRev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for removeRemovedNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeRemovedNodeTwoBranches() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev1 = mk.branch(rev);
        branchRev1 = mk.commit("/", "-\"foo\"", branchRev1, null);

        String branchRev2 = mk.branch(rev);
        // will mark collision on branchRev1
        branchRev2 = mk.commit("/", "-\"foo\"", branchRev2, null);
        mk.merge(branchRev2, null);

        try {
            mk.merge(branchRev1, null);
            fail("Must fail with conflict for removeRemovedNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeChangedNode() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        mk.commit("/foo", "^\"prop\":\"value\"", rev, null);

        try {
            mk.commit("/", "-\"foo\"", rev, null);
            fail("Must fail with conflict for removeChangedNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeChangedNodeBranchWins() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev = mk.branch(rev);
        branchRev = mk.commit("/foo", "^\"prop\":\"value\"", branchRev, null);
        mk.merge(branchRev, null);

        try {
            mk.commit("/", "-\"foo\"", rev, null);
            fail("Must fail with conflict for removeChangedNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeChangedNodeBranchLoses1() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev = mk.branch(rev);
        // branch commit happens before trunk commit
        branchRev = mk.commit("/foo", "^\"prop\":\"value\"", branchRev, null);
        mk.commit("/", "-\"foo\"", rev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for removeChangedNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeChangedNodeBranchLoses2() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev = mk.branch(rev);
        mk.commit("/", "-\"foo\"", rev, null);
        // branch commit happens after trunk commit
        branchRev = mk.commit("/foo", "^\"prop\":\"value\"", branchRev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for removeChangedNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeChangedNodeTwoBranches() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev1 = mk.branch(rev);
        branchRev1 = mk.commit("/", "-\"foo\"", branchRev1, null);

        String branchRev2 = mk.branch(rev);
        // will mark collision on branchRev1
        branchRev2 = mk.commit("/foo", "^\"prop\":\"value\"", branchRev2, null);
        mk.merge(branchRev2, null);

        try {
            mk.merge(branchRev1, null);
            fail("Must fail with conflict for removeChangedNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void changeRemovedNode() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        mk.commit("/", "-\"foo\"", rev, null);

        try {
            mk.commit("/foo", "^\"prop\":\"value\"", rev, null);
            fail("Must fail with conflict for changeRemovedNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void changeRemovedNodeBranchWins() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev = mk.branch(rev);
        branchRev = mk.commit("/", "-\"foo\"", branchRev, null);
        mk.merge(branchRev, null);

        try {
            mk.commit("/foo", "^\"prop\":\"value\"", rev, null);
            fail("Must fail with conflict for changeRemovedNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void changeRemovedNodeBranchLoses1() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev = mk.branch(rev);
        // branch commit happens before trunk commit
        branchRev = mk.commit("/", "-\"foo\"", branchRev, null);
        mk.commit("/foo", "^\"prop\":\"value\"", rev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for changeRemovedNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void changeRemovedNodeBranchLoses2() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev = mk.branch(rev);
        mk.commit("/foo", "^\"prop\":\"value\"", rev, null);
        // branch commit happens after trunk commit
        branchRev = mk.commit("/", "-\"foo\"", branchRev, null);

        try {
            mk.merge(branchRev, null);
            fail("Must fail with conflict for changeRemovedNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void changeRemovedNodeTwoBranches() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        String branchRev1 = mk.branch(rev);
        branchRev1 = mk.commit("/foo", "^\"prop\":\"value\"", branchRev1, null);

        String branchRev2 = mk.branch(rev);
        // will mark collision on branchRev1
        branchRev2 = mk.commit("/", "-\"foo\"", branchRev2, null);
        mk.merge(branchRev2, null);

        try {
            mk.merge(branchRev1, null);
            fail("Must fail with conflict for changeRemovedNode");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void nonConflictingChangeProperty() {
        String rev = mk.commit("/", "+\"foo\":{\"prop1\":\"value\", \"prop2\":\"value\"}", null, null);
        mk.commit("/foo", "^\"prop1\":\"bar\"", rev, null);
        mk.commit("/foo", "^\"prop2\":\"baz\"", rev, null);
    }

    @Test
    public void nonConflictingChangePropertyWithBranch1() {
        String rev = mk.commit("/", "+\"foo\":{\"prop1\":\"value\", \"prop2\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        // branch commit happens before trunk commit
        branchRev = mk.commit("/foo", "^\"prop1\":\"bar\"", branchRev, null);
        mk.commit("/foo", "^\"prop2\":\"baz\"", rev, null);
        mk.merge(branchRev, null);
    }

    @Test
    public void nonConflictingChangePropertyWithBranch2() {
        String rev = mk.commit("/", "+\"foo\":{\"prop1\":\"value\", \"prop2\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        mk.commit("/foo", "^\"prop2\":\"baz\"", rev, null);
        // branch commit happens after trunk commit
        branchRev = mk.commit("/foo", "^\"prop1\":\"bar\"", branchRev, null);
        mk.merge(branchRev, null);
    }

    @Test
    public void nonConflictingAddProperty() {
        String rev = mk.commit("/", "+\"foo\":{\"prop1\":\"value\"}", null, null);
        mk.commit("/foo", "^\"prop1\":\"bar\"", rev, null);
        mk.commit("/foo", "^\"prop2\":\"baz\"", rev, null);
    }

    @Test
    public void nonConflictingAddPropertyWithBranch1() {
        String rev = mk.commit("/", "+\"foo\":{\"prop1\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        // branch commit happens before trunk commit
        branchRev = mk.commit("/foo", "^\"prop1\":\"bar\"", branchRev, null);
        mk.commit("/foo", "^\"prop2\":\"baz\"", rev, null);
        mk.merge(branchRev, null);
    }

    @Test
    public void nonConflictingAddPropertyWithBranch2() {
        String rev = mk.commit("/", "+\"foo\":{\"prop1\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        mk.commit("/foo", "^\"prop2\":\"baz\"", rev, null);
        // branch commit happens after trunk commit
        branchRev = mk.commit("/foo", "^\"prop1\":\"bar\"", branchRev, null);
        mk.merge(branchRev, null);
    }

    @Test
    public void nonConflictingRemoveProperty() {
        String rev = mk.commit("/", "+\"foo\":{\"prop1\":\"value\", \"prop2\":\"value\"}", null, null);
        mk.commit("/foo", "^\"prop1\":\"bar\"", rev, null);
        mk.commit("/foo", "^\"prop2\":null", rev, null);
    }

    @Test
    public void nonConflictingRemovePropertyWithBranch1() {
        String rev = mk.commit("/", "+\"foo\":{\"prop1\":\"value\", \"prop2\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        // branch commit happens before trunk commit
        branchRev = mk.commit("/foo", "^\"prop1\":\"bar\"", branchRev, null);
        mk.commit("/foo", "^\"prop2\":null", rev, null);
        mk.merge(branchRev, null);
    }

    @Test
    public void nonConflictingRemovePropertyWithBranch2() {
        String rev = mk.commit("/", "+\"foo\":{\"prop1\":\"value\", \"prop2\":\"value\"}", null, null);
        String branchRev = mk.branch(rev);
        mk.commit("/foo", "^\"prop2\":null", rev, null);
        // branch commit happens after trunk commit
        branchRev = mk.commit("/foo", "^\"prop1\":\"bar\"", branchRev, null);
        mk.merge(branchRev, null);
    }
}
