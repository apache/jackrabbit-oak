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

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * A set of simple cluster tests.
 */
public class Cluster2Test {

    @Test
    public void twoNodes() throws Exception {
        MemoryDocumentStore ds = new MemoryDocumentStore();
        MemoryBlobStore bs = new MemoryBlobStore();
        DocumentMK.Builder builder;

        builder = new DocumentMK.Builder();
        builder.setDocumentStore(ds).setBlobStore(bs);
        DocumentMK mk1 = builder.setClusterId(1).open();
        builder = new DocumentMK.Builder();
        builder.setDocumentStore(ds).setBlobStore(bs);
        DocumentMK mk2 = builder.setClusterId(2).open();

        mk1.commit("/", "+\"test\":{\"x\": 1}", null, null);
        mk1.backgroundWrite();

        mk2.backgroundRead();
        String b1 = mk2.branch(mk2.getHeadRevision());
        mk2.commit("/", "-\"test\"", b1, null);
        String b2 = mk2.branch(mk2.getHeadRevision());
        String b2b = mk2.commit("/", "-\"test\"", b2, null);
        mk2.merge(b2b, null);
        mk2.backgroundWrite();

        mk1.backgroundRead();
        mk1.commit("/", "+\"test\":{\"x\": 1}", null, null);
        mk1.backgroundWrite();

        mk2.backgroundRead();

        String n1 = mk1.getNodes("/test", mk1.getHeadRevision(), 0, 0, 10, null);
        String n2 = mk2.getNodes("/test", mk2.getHeadRevision(), 0, 0, 10, null);

        // mk1 now sees both changes
        assertEquals("{\"x\":1,\":childNodeCount\":0}", n1);
        assertEquals("{\"x\":1,\":childNodeCount\":0}", n2);

        mk1.dispose();
        mk2.dispose();
    }

}
