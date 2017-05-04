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

package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import java.io.InputStream;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexConsistencyChecker.Level;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexConsistencyChecker.Result;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.*;

public class IndexConsistencyCheckerTest {

    @Test
    public void emptyIndex() throws Exception{
        IndexConsistencyChecker checker = new IndexConsistencyChecker(EMPTY_NODE, "/foo");
        Result result = checker.check(Level.BLOBS_ONLY);
        assertFalse(result.clean);
        assertTrue(result.typeMismatch);
        assertEquals(result.indexPath, "/foo");
    }

    @Test
    public void blobsWithError() throws Exception{
        FailingBlob failingBlob = new FailingBlob("foo");
        IndexDefinitionBuilder defnBuilder = new IndexDefinitionBuilder();

        NodeBuilder idx = defnBuilder.build().builder();
        idx.setProperty("foo", failingBlob);
        idx.child(":index").setProperty("foo", failingBlob);
        idx.child("b").setProperty("foo", Lists.newArrayList(failingBlob, failingBlob), Type.BINARIES);

        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("a", idx.getNodeState());

        IndexConsistencyChecker checker = new IndexConsistencyChecker(builder.getNodeState(), "/a");
        Result result = checker.check(Level.BLOBS_ONLY);

        assertFalse(result.clean);
        assertTrue(result.missingBlobs);
        assertFalse(result.blobSizeMismatch);
        assertEquals(4, result.invalidBlobIds.size());
    }

    @Test
    public void blobsWithSizeMismatch() throws Exception{
        FailingBlob failingBlob = new FailingBlob("foo", true);
        IndexDefinitionBuilder defnBuilder = new IndexDefinitionBuilder();

        NodeBuilder idx = defnBuilder.build().builder();
        idx.child(":index").setProperty("foo", failingBlob);

        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("a", idx.getNodeState());

        IndexConsistencyChecker checker = new IndexConsistencyChecker(builder.getNodeState(), "/a");
        Result result = checker.check(Level.BLOBS_ONLY);

        assertFalse(result.clean);
        assertFalse(result.missingBlobs);
        assertTrue(result.blobSizeMismatch);
        assertEquals(1, result.invalidBlobIds.size());
    }

    private static class FailingBlob extends ArrayBasedBlob {
        static int count;
        private final String id;
        private final boolean corruptLength;

        public FailingBlob(String s) {
           this(s, false);
        }

        public FailingBlob(String s, boolean corruptLength) {
            super(s.getBytes());
            this.id = String.valueOf(++count);
            this.corruptLength = corruptLength;
        }

        @Nonnull
        @Override
        public InputStream getNewStream() {
            if (corruptLength){
                return super.getNewStream();
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public String getContentIdentity() {
            return id;
        }

        @Override
        public long length() {
            return corruptLength ? super.length() + 1 : super.length();
        }
    }


}