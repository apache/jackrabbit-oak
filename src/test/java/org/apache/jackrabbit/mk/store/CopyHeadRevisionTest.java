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

import java.io.File;

import org.apache.jackrabbit.mk.MicroKernelImpl;
import org.apache.jackrabbit.mk.Repository;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.fs.FileUtils;
import org.apache.jackrabbit.mk.json.fast.Jsop;
import org.apache.jackrabbit.mk.json.fast.JsopArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Use-case: start off a new revision store that contains just the head revision
 * and its nodes.
 * 
 * TODO: make the test concurrent
 */
public class CopyHeadRevisionTest {

    @Before
    public void setup() throws Exception {
        FileUtils.deleteRecursive("target/mk1", false);
        FileUtils.deleteRecursive("target/mk2", false);
    }

    @After
    public void tearDown() throws Exception {
    }
    
    @Test
    public void testCopyHeadRevisionToNewStore() throws Exception {
        String[] revs = new String[5];
        
        DefaultRevisionStore rsFrom = new DefaultRevisionStore();
        rsFrom.initialize(new File("target/mk1"));

        DefaultRevisionStore rsTo = new DefaultRevisionStore(); 
        rsTo.initialize(new File("target/mk2"));

        CopyingGC gc = new CopyingGC(rsFrom, rsTo);
        
        MicroKernel mk = new MicroKernelImpl(new Repository(gc));
        revs[0] = mk.commit("/", "+\"a\" : { \"c\":{}, \"d\":{} }", mk.getHeadRevision(), null);
        revs[1] = mk.commit("/", "+\"b\" : {}", mk.getHeadRevision(), null);
        revs[2] = mk.commit("/b", "+\"e\" : {}", mk.getHeadRevision(), null);
        revs[3] = mk.commit("/a/c", "+\"f\" : {}", mk.getHeadRevision(), null);

        // Simulate a GC cycle start
        gc.start();

        revs[4] = mk.commit("/b/e", "+\"g\" : {}", mk.getHeadRevision(), null);
        
        // Simulate a GC cycle stop
        gc.stop();
        
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
}
