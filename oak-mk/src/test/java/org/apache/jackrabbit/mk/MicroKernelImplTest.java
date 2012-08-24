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
package org.apache.jackrabbit.mk;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class MicroKernelImplTest {
    
    private File homeDir;
    private MicroKernelImpl mk;
    
    @Before
    public void setup() throws Exception {
        homeDir = new File("target/mk");
        if (homeDir.exists()) {
            FileUtils.cleanDirectory(homeDir);
        }
        mk = new MicroKernelImpl(homeDir.getPath());
    }
    
    @After
    public void tearDown() throws Exception {
        if (mk != null) {
            mk.dispose();
        }
    }
    
    /**
     * OAK-276: potential clash of commit id's after restart.
     */
    @Test
    public void potentialClashOfCommitIds() {
        String headRev = mk.commit("/", "+\"a\" : {}", mk.getHeadRevision(), null);
        String branchRev = mk.branch(mk.getHeadRevision());
        
        mk.dispose();
        mk = new MicroKernelImpl(homeDir.getPath());
        assertEquals("Stored head should be equal", headRev, mk.getHeadRevision());

        headRev = mk.commit("/", "+\"b\" : {}", mk.getHeadRevision(), null);
        assertFalse("Commit must not have same id as branch", headRev.equals(branchRev));
    }
}
