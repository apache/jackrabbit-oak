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
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import java.nio.ByteBuffer;
import java.util.Collections;

import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.NamePathRev;
import org.apache.jackrabbit.oak.plugins.document.PathRev;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.h2.mvstore.WriteBuffer;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DataTypeUtilTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private WriteBuffer wb = new WriteBuffer(1024);

    @Test
    public void booleanToBufferTrue() {
        DataTypeUtil.booleanToBuffer(true, wb);
        ByteBuffer rb = readBufferFrom(wb);
        assertTrue(DataTypeUtil.booleanFromBuffer(rb));
    }

    @Test
    public void booleanToBufferFalse() {
        DataTypeUtil.booleanToBuffer(false, wb);
        ByteBuffer rb = readBufferFrom(wb);
        assertFalse(DataTypeUtil.booleanFromBuffer(rb));
    }

    @Test
    public void revisionVectorToBufferEmpty() {
        RevisionVector empty = new RevisionVector();
        DataTypeUtil.revisionVectorToBuffer(empty, wb);
        ByteBuffer rb = readBufferFrom(wb);
        assertEquals(empty, DataTypeUtil.revisionVectorFromBuffer(rb));
    }

    @Test
    public void revisionVectorToBuffer() {
        RevisionVector revisions = RevisionVector.fromString("r9-0-1,br7-0-2");
        DataTypeUtil.revisionVectorToBuffer(revisions, wb);
        ByteBuffer rb = readBufferFrom(wb);
        assertEquals(revisions, DataTypeUtil.revisionVectorFromBuffer(rb));
    }

    @Test
    public void pathToBufferRoot() {
        Path p = Path.ROOT;
        DataTypeUtil.pathToBuffer(p, wb);
        ByteBuffer rb = readBufferFrom(wb);
        assertEquals(p, DataTypeUtil.pathFromBuffer(rb));
    }

    @Test
    public void pathToBuffer() {
        Path p = Path.fromString("/foo/bar/quux");
        DataTypeUtil.pathToBuffer(p, wb);
        ByteBuffer rb = readBufferFrom(wb);
        assertEquals(p, DataTypeUtil.pathFromBuffer(rb));
    }

    @Test
    public void relPathToBuffer() {
        Path p = Path.fromString("foo/bar/quux");
        DataTypeUtil.pathToBuffer(p, wb);
        ByteBuffer rb = readBufferFrom(wb);
        assertEquals(p, DataTypeUtil.pathFromBuffer(rb));
    }

    @Test
    public void relPathSingleElementToBuffer() {
        Path p = Path.fromString("foo");
        DataTypeUtil.pathToBuffer(p, wb);
        ByteBuffer rb = readBufferFrom(wb);
        assertEquals(p, DataTypeUtil.pathFromBuffer(rb));
    }

    @Test
    public void relPathMultipleToBuffer() {
        Path fooBar = Path.fromString("foo/bar");
        Path barBaz = Path.fromString("bar/baz");
        DataTypeUtil.pathToBuffer(fooBar, wb);
        DataTypeUtil.pathToBuffer(barBaz, wb);
        ByteBuffer rb = readBufferFrom(wb);
        assertEquals(fooBar, DataTypeUtil.pathFromBuffer(rb));
        assertEquals(barBaz, DataTypeUtil.pathFromBuffer(rb));
    }

    @Test
    public void pathRevToBuffer() {
        Path p = Path.fromString("/foo/bar/quux");
        RevisionVector rv = RevisionVector.fromString("r3-4-1,br4-9-2");
        PathRev expected = new PathRev(p, rv);
        DataTypeUtil.pathRevToBuffer(expected, wb);
        ByteBuffer rb = readBufferFrom(wb);
        assertEquals(expected, DataTypeUtil.pathRevFromBuffer(rb));
    }

    @Test
    public void pathNameRevToBuffer() {
        Path p = Path.fromString("/foo/bar/quux");
        String name = "baz";
        RevisionVector rv = RevisionVector.fromString("r3-4-1,br4-9-2");
        NamePathRev expected = new NamePathRev(name, p, rv);
        DataTypeUtil.namePathRevToBuffer(expected, wb);
        ByteBuffer rb = readBufferFrom(wb);
        assertEquals(expected, DataTypeUtil.namePathRevFromBuffer(rb));
    }

    @Test
    public void stateToBufferLastRevNull() {
        DocumentNodeStore ns = builderProvider.newBuilder().build();
        Path p = Path.fromString("/foo/bar");
        RevisionVector rootRev = ns.getHeadRevision();
        DocumentNodeState expected = new DocumentNodeState(ns, p, rootRev,
                Collections.emptyMap(), true, 0, null, false);
        DataTypeUtil.stateToBuffer(expected, wb);
        ByteBuffer rb = readBufferFrom(wb);
        assertEquals(expected, DataTypeUtil.stateFromBuffer(ns, rb));
    }

    @Test
    public void stateToBuffer() {
        DocumentNodeStore ns = builderProvider.newBuilder().build();
        Path p = Path.fromString("/foo/bar");
        RevisionVector rootRev = ns.getHeadRevision();
        DocumentNodeState expected = new DocumentNodeState(ns, p, rootRev,
                Collections.emptyMap(), true, 0, rootRev, false);
        DataTypeUtil.stateToBuffer(expected, wb);
        ByteBuffer rb = readBufferFrom(wb);
        assertEquals(expected, DataTypeUtil.stateFromBuffer(ns, rb));
    }

    private static ByteBuffer readBufferFrom(WriteBuffer wb) {
        ByteBuffer rb = wb.getBuffer();
        rb.rewind();
        return rb;
    }
}
