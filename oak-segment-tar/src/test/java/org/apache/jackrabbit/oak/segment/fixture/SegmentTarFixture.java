/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.segment.fixture;

import java.io.IOException;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class SegmentTarFixture extends NodeStoreFixture {
    private final SegmentNodeStore sns;

    public SegmentTarFixture() {
        this(null);
    }
    
    public SegmentTarFixture(SegmentNodeStore sns) {
        this.sns = sns;
    }
    
    @Override
    public NodeStore createNodeStore() {
        if (sns == null) {
            try {
                return SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return sns;
        }
    }

    @Override
    public String toString() {
        return "SegmentTar";
    }
}