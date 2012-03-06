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
package org.apache.jackrabbit.mk.cluster;

import org.apache.jackrabbit.mk.MicroKernelFactory;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HotBackupTest {

    private MicroKernel source; 
    private MicroKernel target; 
    
    @Before
    public void setUp() throws Exception {
        source = MicroKernelFactory.getInstance("fs:{homeDir}/target/mk1;clean");
        target = MicroKernelFactory.getInstance("fs:{homeDir}/target/mk2;clean");
    }
    
    @After
    public void tearDown() {
        if (source != null) {
            source.dispose();
        }
        if (target != null) {
            target.dispose();
        }
    }
    
    @Test
    public void test() {
        HotBackup hotbackup = new HotBackup(source, target);
        source.commit("/", "+\"test\":{}", source.getHeadRevision(), null);
        hotbackup.sync();
        target.getNodes("/test", target.getHeadRevision());
    }
}
