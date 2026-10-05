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
package org.apache.jackrabbit.oak.segment.consensus.server;

import java.util.List;

import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronClusterLauncher;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;

public final class AeronClusterStartupResult {
    private final AeronConsensusEngine aeronEngine;
    private final AeronClusterLauncher launcher;
    private final List<String> hostnames;
    private final int nodeId;

    public AeronClusterStartupResult(AeronConsensusEngine aeronEngine,
                                     AeronClusterLauncher launcher,
                                     List<String> hostnames,
                                     int nodeId) {
        this.aeronEngine = aeronEngine;
        this.launcher = launcher;
        this.hostnames = hostnames;
        this.nodeId = nodeId;
    }

    public AeronConsensusEngine getAeronEngine() {
        return aeronEngine;
    }

    public AeronClusterLauncher getLauncher() {
        return launcher;
    }

    public List<String> getHostnames() {
        return hostnames;
    }

    public int getNodeId() {
        return nodeId;
    }
}
