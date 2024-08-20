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
package org.apache.jackrabbit.oak.index.indexer.document.tree.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class PageFileTest {

    @Test
    public void serializeLeafNode() {
        PageFile f = new PageFile(false, 1_000_000);
        f.appendRecord("test", null);
        f.appendRecord("", "");
        f.appendRecord(new String(new char[8000]) + "x", new String(new char[80000]) + "y");
        f.appendRecord("a", "b");
        f.setUpdate(-3);
        byte[] data = f.toBytes();
        PageFile f2 = PageFile.fromBytes(data, 1_000_000);
        assertTrue(f.isInnerNode() == f2.isInnerNode());
        for (int i = 0; i < f.getKeys().size(); i++) {
            assertEquals(f.getKey(i), f2.getKey(i));
            assertEquals(f.getValue(i), f2.getValue(i));
        }
        assertEquals(f.getUpdate(), f2.getUpdate());
    }

    @Test
    public void serializeInnerNode() {
        PageFile f = new PageFile(true, 1_000_000);
        f.appendRecord("test", null);
        f.appendRecord("", "");
        f.appendRecord(new String(new char[8000]) + "x", new String(new char[80000]) + "y");
        f.appendRecord("a", "b");
        f.setUpdate(-3);
        byte[] data = f.toBytes();
        PageFile f2 = PageFile.fromBytes(data, 1_000_000);
        assertTrue(f.isInnerNode() == f2.isInnerNode());
        for (int i = 0; i < f.getKeys().size(); i++) {
            assertEquals(f.getKey(i), f2.getKey(i));
            assertEquals(f.getValue(i), f2.getValue(i));
        }
        assertEquals(f.getUpdate(), f2.getUpdate());
    }

}
