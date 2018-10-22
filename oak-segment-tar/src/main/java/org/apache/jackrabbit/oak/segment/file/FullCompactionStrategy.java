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

package org.apache.jackrabbit.oak.segment.file;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType.FULL;

import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;

class FullCompactionStrategy extends AbstractCompactionStrategy {

    @Override
    GCType getCompactionType() {
        return FULL;
    }

    @Override
    GCGeneration nextGeneration(GCGeneration current) {
        return current.nextFull();
    }

    @Override
    public CompactionResult compact(Context context) {
        return compact(context, EMPTY_NODE);
    }

}
