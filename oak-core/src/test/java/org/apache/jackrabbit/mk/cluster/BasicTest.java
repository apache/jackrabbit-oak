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
import org.apache.jackrabbit.mk.server.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class BasicTest {
    
    private MicroKernel mk1;
    private Server server;
    
    private MicroKernel mk2;
    
    @Before
    public void setUp() throws Exception {
        mk1 = MicroKernelFactory.getInstance("fs:{homeDir}/target/mk1;clean");
        mk2 = MicroKernelFactory.getInstance("fs:{homeDir}/target/mk2;clean");
    }
    
    @After
    public void tearDown() {
        if (mk1 != null) {
            mk1.dispose();
        }
        if (mk2 != null) {
            mk2.dispose();
        }
        if (server != null) {
            server.stop();
        }
    }
    
    @Test
    @Ignore
    public void test() throws Exception {
        server = new Server(mk1);
        server.setPort(0);
        server.start();
        
        ClusterNode cn = new ClusterNode(mk2);
        cn.join(server.getAddress());
                
        mk1.commit("/", "+\"test\":{}", mk1.getHeadRevision(), null);
        mk2.getNodes("/test", mk2.getHeadRevision());
    }
}
