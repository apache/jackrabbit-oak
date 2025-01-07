/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.jackrabbit.oak.composite.it;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import javax.inject.Inject;

import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.options;

/**
 * Validates composite multi-mount configuration using {@link org.apache.jackrabbit.oak.composite.MountInfoConfig}.
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class MultiMountCompositeIT extends CompositeTestSupport {

    @Configuration
    public Option[] configuration() {
        NodeStoresInitializer.initTestStores();

        return options(baseConfiguration());
    }

    protected Path getConfigDir() {
        return Paths.get("src", "test", "resources", "it", "multi-mounts");
    }

    @Inject
    private NodeStore store;

    @Test
    public void compositeNodeStoreWithMultipleReadOnlyMounts() {
        assertEquals("Node store should be a CompositeNodeStore", "CompositeNodeStore", store.getClass().getSimpleName());

        NodeState root = store.getRoot();
        Set<String> expectedNodes = Set.of("content", "apps", "libs");
        Set<String> actualNodes = CollectionUtils.toSet(root.getChildNodeNames());
        assertTrue("Expected nodes " + expectedNodes + ", but was " + actualNodes, actualNodes.containsAll(expectedNodes));

        assertTrue("'apps' mount should be mounted", root.getChildNode("apps").getChildNode("appsMount").exists());
        assertTrue("'libs' path should be mounted", root.getChildNode("libs").getChildNode("libsMount").exists());
        assertTrue("'global' mount should be mounted", root.getChildNode("content").getChildNode("globalMount").exists());
    }
}
