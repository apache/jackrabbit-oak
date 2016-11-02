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

package org.apache.jackrabbit.oak;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.fixture.DocumentMemoryFixture;
import org.apache.jackrabbit.oak.fixture.DocumentMongoFixture;
import org.apache.jackrabbit.oak.fixture.DocumentRdbFixture;
import org.apache.jackrabbit.oak.fixture.MemoryFixture;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.plugins.multiplex.MultiplexingMemoryFixture;
import org.apache.jackrabbit.oak.plugins.multiplex.MultiplexingSegmentFixture;
import org.apache.jackrabbit.oak.plugins.segment.fixture.SegmentFixture;
import org.apache.jackrabbit.oak.segment.fixture.SegmentTarFixture;

public class NodeStoreFixtures {

    public static final NodeStoreFixture MEMORY_NS = new MemoryFixture();

    public static final NodeStoreFixture SEGMENT_MK = new SegmentFixture();

    public static final NodeStoreFixture SEGMENT_TAR = new SegmentTarFixture();

    public static final NodeStoreFixture DOCUMENT_NS = new DocumentMongoFixture();

    public static final NodeStoreFixture DOCUMENT_RDB = new DocumentRdbFixture();

    public static final NodeStoreFixture DOCUMENT_MEM = new DocumentMemoryFixture();

    public static final NodeStoreFixture MULTIPLEXED_SEGMENT = new MultiplexingSegmentFixture();

    public static final NodeStoreFixture MULTIPLEXED_MEM = new MultiplexingMemoryFixture();

    public static Collection<Object[]> asJunitParameters(Set<FixturesHelper.Fixture> fixtures) {
        List<NodeStoreFixture> configuredFixtures = new ArrayList<NodeStoreFixture>();
        if (fixtures.contains(FixturesHelper.Fixture.DOCUMENT_NS)) {
            configuredFixtures.add(DOCUMENT_NS);
        }
        if (fixtures.contains(FixturesHelper.Fixture.SEGMENT_MK)) {
            configuredFixtures.add(SEGMENT_MK);
        }
        if (fixtures.contains(FixturesHelper.Fixture.MEMORY_NS)) {
            configuredFixtures.add(MEMORY_NS);
        }
        if (fixtures.contains(FixturesHelper.Fixture.DOCUMENT_RDB)) {
            configuredFixtures.add(DOCUMENT_RDB);
        }
        if (fixtures.contains(FixturesHelper.Fixture.DOCUMENT_MEM)) {
            configuredFixtures.add(DOCUMENT_MEM);
        }
        if (fixtures.contains(FixturesHelper.Fixture.SEGMENT_TAR)) {
            configuredFixtures.add(SEGMENT_TAR);
        }
        if (fixtures.contains(FixturesHelper.Fixture.MULTIPLEXED_SEGMENT)) {
            configuredFixtures.add(MULTIPLEXED_SEGMENT);
        }
        if (fixtures.contains(FixturesHelper.Fixture.MULTIPLEXED_MEM)) {
            configuredFixtures.add(MULTIPLEXED_MEM);
        }

        Collection<Object[]> result = new ArrayList<Object[]>();
        for (NodeStoreFixture f : configuredFixtures) {
            if (f.isAvailable()) {
                result.add(new Object[]{f});
            }
        }
        return result;
    }

}
