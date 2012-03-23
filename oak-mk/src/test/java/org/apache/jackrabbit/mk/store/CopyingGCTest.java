/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.blobs.MemoryBlobStore;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.core.Repository;
import org.apache.jackrabbit.mk.json.fast.Jsop;
import org.apache.jackrabbit.mk.json.fast.JsopArray;
import org.apache.jackrabbit.mk.persistence.InMemPersistence;
import org.apache.jackrabbit.mk.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Use-case: start off a new revision store that contains just the head revision
 * and its nodes.
 * 
 * TODO: fix concurrent GC, fails because of missing nodes in the "to" store.
 */
public class CopyingGCTest {

    private DefaultRevisionStore rsFrom;
    private DefaultRevisionStore rsTo;
    
    @Before
    public void setup() throws Exception {
        delete(new File("target/mk1"));
        delete(new File("target/mk2"));
    }

    @After
    public void tearDown() throws Exception {
        IOUtils.closeQuietly(rsFrom);
        IOUtils.closeQuietly(rsTo);
    }
    
    @Ignore
    @Test
    public void concurrentGC() throws Exception {
        rsFrom = new DefaultRevisionStore(new InMemPersistence());
        rsFrom.initialize();

        rsTo = new DefaultRevisionStore(new InMemPersistence()); 
        rsTo.initialize();

        final CopyingGC gc = new CopyingGC(rsFrom, rsTo);
        
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    gc.gc();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        t.setDaemon(true);

        MicroKernel mk = new MicroKernelImpl(new Repository(gc, new MemoryBlobStore()));
        mk.commit("/", "+\"a\" : { \"b\" : { \"c\" : { \"d\" : {} } } }", mk.getHeadRevision(), null);
        
        t.start();
        
        try {
            for (int i = 0; i < 10; i++) {
                mk.commit("/a/b/c/d", "+\"e\" : {}", mk.getHeadRevision(), null);
                Thread.sleep(100);
                mk.commit("/a/b/c/d", "-\"e\"", mk.getHeadRevision(), null);
            }
        } finally {        
            t.join();
        }
    }
    
    @Test
    public void copyHeadRevisionToNewStore() throws Exception {
        String[] revs = new String[5];
        
        rsFrom = new DefaultRevisionStore(new InMemPersistence());
        rsFrom.initialize();

        rsTo = new DefaultRevisionStore(new InMemPersistence()); 
        rsTo.initialize();

        CopyingGC gc = new CopyingGC(rsFrom, rsTo);
        
        MicroKernel mk = new MicroKernelImpl(new Repository(gc, new MemoryBlobStore()));
        revs[0] = mk.commit("/", "+\"a\" : { \"c\":{}, \"d\":{} }", mk.getHeadRevision(), null);
        revs[1] = mk.commit("/", "+\"b\" : {}", mk.getHeadRevision(), null);
        revs[2] = mk.commit("/b", "+\"e\" : {}", mk.getHeadRevision(), null);
        revs[3] = mk.commit("/a/c", "+\"f\" : {}", mk.getHeadRevision(), null);

        // garbage collect
        gc.gc();

        revs[4] = mk.commit("/b/e", "+\"g\" : {}", mk.getHeadRevision(), null);
        
        // Assert head revision is contained after GC
        assertEquals(mk.getHeadRevision(), revs[revs.length - 1]);
        
        // Assert unused revisions were GCed
        for (int i = 0; i < 3; i++) {
            try {
                mk.getNodes("/", revs[i]);
                fail("Revision should have been GCed: "+ revs[i]);
            } catch (MicroKernelException e) {
                // ignore
            }
        }
        
        // Assert MK contains 3 revisions only
        assertEquals(3, ((JsopArray) Jsop.parse(mk.getRevisions(0, Integer.MAX_VALUE))).size());
    }
    
    private static void delete(File f) throws IOException {
        if (f.exists()) {
            if (f.isDirectory()) {
                // recursively delete children first
                for (File child : f.listFiles()) {
                    delete(child);
                }
            }
            f.delete();
        }
    }
}
