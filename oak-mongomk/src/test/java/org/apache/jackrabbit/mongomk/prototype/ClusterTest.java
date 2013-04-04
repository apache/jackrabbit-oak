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
package org.apache.jackrabbit.mongomk.prototype;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.blobs.MemoryBlobStore;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.DB;

/**
 * A set of simple cluster tests.
 */
public class ClusterTest {
    
    private static final boolean MONGO_DB = false;
    // private static final boolean MONGO_DB = true;
    
    private MemoryDocumentStore ds;
    private MemoryBlobStore bs;
    
    @Test
    public void conflict() {
        MongoMK mk1 = createMK(1);
        MongoMK mk2 = createMK(2);
        
        String m1r0 = mk1.getHeadRevision();
        String m2r0 = mk2.getHeadRevision();
        
        mk1.commit("/", "+\"test\":{}", m1r0, null);
        try {
            mk2.commit("/", "+\"test\":{}", m2r0, null);
            fail();
        } catch (MicroKernelException e) {
            // expected
        }
        
        mk1.dispose();
        mk2.dispose();
    }
    
    @Test
    @Ignore
    public void revisionVisibility() throws InterruptedException {
        MongoMK mk1 = createMK(1);
        MongoMK mk2 = createMK(2);
        
        String m2h;
        m2h = mk2.getNodes("/", mk2.getHeadRevision(), 0, 0, 2, null);
        assertEquals("{\":childNodeCount\":0}", m2h);
        
        mk1.commit("/", "+\"test\":{}", null, null);
        String m1h = mk1.getNodes("/", mk1.getHeadRevision(), 0, 0, 1, null);
        assertEquals("{\"test\":{},\":childNodeCount\":1}", m1h);
        
        m2h = mk2.getNodes("/", mk2.getHeadRevision(), 0, 0, 2, null);
        // not available yet...
        assertEquals("{\":childNodeCount\":0}", m2h);
        
        // the delay is 10 ms
        Thread.sleep(100);
        
        // so now it should be available
        m2h = mk2.getNodes("/", mk2.getHeadRevision(), 0, 0, 5, null);
        assertEquals("{\"test\":{},\":childNodeCount\":1}", m2h);
        
        mk1.dispose();
        mk2.dispose();
    }    
    
    @Test
    public void rollbackAfterConflict() {
        MongoMK mk1 = createMK(1);
        MongoMK mk2 = createMK(2);
        
        String m1r0 = mk1.getHeadRevision();
        String m2r0 = mk2.getHeadRevision();
        
        mk1.commit("/", "+\"test\":{}", m1r0, null);
        try {
            mk2.commit("/", "+\"a\": {} +\"test\":{}", m2r0, null);
            fail();
        } catch (MicroKernelException e) {
            // expected
        }
        mk2.commit("/", "+\"a\": {}", null, null);
        
        mk1.dispose();
        mk2.dispose();
    }


    private MongoMK createMK(int clusterId) {
        MongoMK.Builder builder = new MongoMK.Builder();
        if (MONGO_DB) {
            DB db = MongoUtils.getConnection().getDB();
            MongoUtils.dropCollections(db);
            builder.setMongoDB(db);
        } else {
            if (ds == null) {
                ds = new MemoryDocumentStore();
            }
            if (bs == null) {
                bs = new MemoryBlobStore();
            }
            builder.setDocumentStore(ds).setBlobStore(bs);
        }
        builder.setAsyncDelay(10);
        return builder.setClusterId(clusterId).open();
    }

}
