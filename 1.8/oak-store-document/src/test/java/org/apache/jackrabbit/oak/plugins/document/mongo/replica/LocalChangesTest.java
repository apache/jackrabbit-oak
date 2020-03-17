/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.mongo.replica;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.junit.Test;

public class LocalChangesTest {

    @Test
    public void testReplicaInactive() {
        LocalChanges l = new LocalChanges(0);
        assertTrue(l.mayContain("2:/xyz/123"));
        assertTrue(l.mayContainChildrenOf("2:/xyz/123"));

        l.add("2:/xyz/123", 2); // don't remember the path, only the 2 timestamp
        l.gotRootRevisions(revsV(1, 1, 1));
        assertTrue(l.mayContain("2:/xyz/123"));
        assertTrue(l.mayContain("2:/abc/567")); // we only remembered the timestamp, not the path

        l.gotRootRevisions(revsV(3, 3, 3)); // the new revision >= the remembered 2
        assertFalse(l.mayContain("2:/xyz/123"));
        assertFalse(l.mayContain("2:/abc/567"));
    }

    @Test
    public void testMayContain() {
        LocalChanges l = new LocalChanges(0);
        l.add("2:/xyz/123", 2);

        l.gotRootRevisions(revsV(1, 1, 1));
        assertTrue(l.mayContain("2:/xyz/123"));
        assertTrue(l.mayContainChildrenOf("1:/xyz"));

        l.gotRootRevisions(revsV(2, 2, 2));
        assertFalse(l.mayContain("2:/xyz/123"));
        assertFalse(l.mayContainChildrenOf("1:/xyz"));
    }

    @Test
    public void testGotRootRevisions() {
        LocalChanges l = new LocalChanges(2); // only consider the last timestamp in revisison
        l.add("2:/xyz/123", 4);

        l.gotRootRevisions(revsV(1, 1, 1));
        assertTrue(l.mayContain("2:/xyz/123"));

        l.gotRootRevisions(revsV(2, 2, 2));
        assertTrue(l.mayContain("2:/xyz/123"));

        l.gotRootRevisions(revsV(2, 3, 3));
        assertTrue(l.mayContain("2:/xyz/123"));

        l.gotRootRevisions(revsV(2, 3, 4));
        assertFalse(l.mayContain("2:/xyz/123"));
    }

    @Test
    public void testLimit() {
        LocalChanges l = new LocalChanges(0);
        l.gotRootRevisions(revsV(1)); // make the class active

        for (int i = 1; i <= 99; i++) {
            l.add("2:/xyz/" + i, i + 100);
            assertTrue(l.mayContain("2:/xyz/" + i));
            assertFalse(l.mayContain("2:/abc/" + i));
        }
        l.add("2:/xyz/100", 200); // the list should be cleared right now
        l.add("2:/abc/123", 300); // this is added to the new list
        l.add("2:/abc/456", 100); // this shouldn't be added to the new list (as it's old)

        // now the list should be cleared and we should got true for all documents
        assertTrue(l.mayContain("2:/abc/999"));
        assertEquals(singleton("2:/abc/123"), l.localChanges.keySet());

        l.gotRootRevisions(revsV(200)); // invalidate
        assertFalse(l.mayContain("2:/xyz/0"));
        assertFalse(l.mayContain("2:/xyz/99"));

        assertTrue(l.mayContain("2:/abc/123"));
        assertFalse(l.mayContain("2:/abc/456"));
    }

    @Test
    public void dontAddOldRevisions() {
        LocalChanges l = new LocalChanges(0);
        l.gotRootRevisions(revsV(10));
        l.add("2:/xyz/1", 5);
        assertFalse(l.mayContain("2:/xyz/1"));
    }

    private Collection<Revision> revs(int... timestamps) {
        List<Revision> revs = new ArrayList<Revision>();
        for (int i = 0; i < timestamps.length; i++) {
            revs.add(new Revision(timestamps[i], 0, i, false));
        }
        return revs;
    }

    private RevisionVector revsV(int... timestamps) {
        return new RevisionVector(revs(timestamps));
    }
}
