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
package org.apache.jackrabbit.oak.plugins.document;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.junit.Test;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ClusterNodeInfoComparatorTest {

    private static final String MACHINE_ID = "machine";
    private static final String INSTANCE_1 = "node-1";
    private static final String INSTANCE_2 = "node-2";

    private DocumentStore store = new MemoryDocumentStore();

    private ClusterNodeInfoComparator comparator =
            new ClusterNodeInfoComparator(MACHINE_ID, INSTANCE_1);

    @Test
    public void lowerClusterIdFirst() {
        SortedSet<ClusterNodeInfo> infos = new TreeSet<>(comparator);
        infos.add(newClusterNodeInfo(1, INSTANCE_1));
        infos.add(newClusterNodeInfo(3, INSTANCE_1));
        infos.add(newClusterNodeInfo(2, INSTANCE_1));
        infos.add(newClusterNodeInfo(4, INSTANCE_1));

        assertThat(idList(infos), contains(1, 2, 3, 4));
    }

    @Test
    public void lowerClusterIdFirstNonMatchingEnvironment() {
        SortedSet<ClusterNodeInfo> infos = new TreeSet<>(comparator);
        infos.add(newClusterNodeInfo(1, INSTANCE_2));
        infos.add(newClusterNodeInfo(3, INSTANCE_2));
        infos.add(newClusterNodeInfo(2, INSTANCE_2));
        infos.add(newClusterNodeInfo(4, INSTANCE_2));

        assertThat(idList(infos), contains(1, 2, 3, 4));
    }

    @Test
    public void matchingEnvironmentFirst() {
        SortedSet<ClusterNodeInfo> infos = new TreeSet<>(comparator);
        infos.add(newClusterNodeInfo(1, INSTANCE_2));
        infos.add(newClusterNodeInfo(2, INSTANCE_2));
        infos.add(newClusterNodeInfo(3, INSTANCE_1));
        infos.add(newClusterNodeInfo(4, INSTANCE_1));
        infos.add(newClusterNodeInfo(5, INSTANCE_2));

        assertThat(idList(infos), contains(3, 4, 1, 2, 5));
    }

    private static List<Integer> idList(Set<ClusterNodeInfo> infos) {
        return infos.stream().map(ClusterNodeInfo::getId).collect(Collectors.toList());
    }

    private ClusterNodeInfo newClusterNodeInfo(int id, String instanceId) {
        try {
            Constructor<ClusterNodeInfo> ctr = ClusterNodeInfo.class.getDeclaredConstructor(
                    int.class, DocumentStore.class, String.class, String.class, boolean.class);
            ctr.setAccessible(true);
            return ctr.newInstance(id, store, MACHINE_ID, instanceId, true);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        throw new IllegalStateException();
    }
}
