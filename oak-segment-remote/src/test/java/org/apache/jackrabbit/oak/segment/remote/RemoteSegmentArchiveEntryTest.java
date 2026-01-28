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
package org.apache.jackrabbit.oak.segment.remote;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class RemoteSegmentArchiveEntryTest {

    private final RemoteSegmentArchiveEntry entry = new RemoteSegmentArchiveEntry(1L, 2L, 0, 128, 3, 4, true);

    @Test
    public void getMsb() {
        assertEquals(1, entry.getMsb());
    }

    @Test
    public void getLsb() {
        assertEquals(2, entry.getLsb());
    }

    @Test
    public void getPosition() {
        assertEquals(0, entry.getPosition());
    }

    @Test
    public void getLength() {
        assertEquals(128, entry.getLength());
    }

    @Test
    public void getGeneration() {
        assertEquals(3, entry.getGeneration());
    }

    @Test
    public void getFullGeneration() {
        assertEquals(4, entry.getFullGeneration());
    }

    @Test
    public void isCompacted() {
        assertTrue(entry.isCompacted());
    }

    @Test
    public void getUuid() {
        assertSame("The same UUID instance must be returned for different calls", entry.getUuid(), entry.getUuid());
        assertEquals(entry.getMsb(), entry.getUuid().getMostSignificantBits());
        assertEquals(entry.getLsb(), entry.getUuid().getLeastSignificantBits());
    }
}
