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
 *
 */

import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder
import org.apache.jackrabbit.oak.spi.commit.EmptyHook
import org.apache.jackrabbit.oak.spi.commit.CommitInfo

void addNodes(SegmentNodeBuilder builder, int count, int depth) {
    if (depth > 0) {
        for (int c = 0; c < count; c++) {
            builder.setProperty(
                    "property-name-" + depth + "-" + c,
                    "property-value-" + depth + "-" + c)
            addNodes(
                    builder.setChildNode("node-" + depth + "-" + c),
                    count,
                    depth - 1)
        }
    }
}

nodeStore = session.store
builder = nodeStore.getRoot().builder()
addNodes(builder, 10, 5)
nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
