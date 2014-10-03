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
package org.apache.jackrabbit.mk.test;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.client.Client;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.server.Server;

import java.net.InetSocketAddress;

public class ClientFixture implements MicroKernelFixture {

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public void setUpCluster(MicroKernel[] cluster) throws Exception {
        InetSocketAddress address = new InetSocketAddress("localhost", 28080);
        cluster[0] = new Client(address);
        for (int i = 1; i < cluster.length; i++) {
            cluster[i] = new Client(address);
        }
    }

    @Override
    public void syncMicroKernelCluster(MicroKernel... nodes) {
    }

    @Override
    public void tearDownCluster(MicroKernel[] cluster) {
    }
}
