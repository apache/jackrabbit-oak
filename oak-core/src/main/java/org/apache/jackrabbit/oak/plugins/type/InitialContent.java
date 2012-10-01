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
package org.apache.jackrabbit.oak.plugins.type;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.spi.lifecycle.DefaultMicroKernelTracker;
import org.apache.jackrabbit.oak.spi.lifecycle.MicroKernelTracker;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * <code>InitialContent</code> implements a {@link MicroKernelTracker} and
 * registers built-in node types when the micro kernel becomes available.
 */
@Component
@Service(MicroKernelTracker.class)
public class InitialContent extends DefaultMicroKernelTracker {

    @Override
    public void available(MicroKernel mk) {
        NodeStore nodeStore = new Oak(mk).createNodeStore();
        // FIXME: depends on CoreValue's name mangling
        NodeState root = nodeStore.getRoot();
        if (root.hasChildNode("jcr:system")) {
            mk.commit("/", "^\"jcr:primaryType\":\"nam:rep:root\" ", null, null);
        } else {
            mk.commit("/", "^\"jcr:primaryType\":\"nam:rep:root\"" +
                    "+\"jcr:system\":{" +
                    "\"jcr:primaryType\"    :\"nam:rep:system\"," +
                    "\":unique\"            :{\"jcr:uuid\":{},\"rep:authorizableId\":{},\"rep:principalName\":{}}," +
                    "\"jcr:versionStorage\" :{\"jcr:primaryType\":\"nam:rep:versionStorage\"}," +
                    "\"jcr:nodeTypes\"      :{\"jcr:primaryType\":\"nam:rep:nodeTypes\"}," +
                    "\"jcr:activities\"     :{\"jcr:primaryType\":\"nam:rep:Activities\"}," +
                    "\"rep:privileges\"     :{\"jcr:primaryType\":\"nam:rep:Privileges\"}}", null, null);
        }

        BuiltInNodeTypes.register(new Oak(mk).createRoot());
    }
}
