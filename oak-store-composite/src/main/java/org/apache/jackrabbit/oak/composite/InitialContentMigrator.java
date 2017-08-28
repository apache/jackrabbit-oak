/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.composite;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.migration.FilteringNodeState;
import org.apache.jackrabbit.oak.plugins.migration.report.LoggingReporter;
import org.apache.jackrabbit.oak.plugins.migration.report.ReportingNodeState;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfo;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InitialContentMigrator {

    private static final int LOG_NODE_COPY = Integer.getInteger("oak.upgrade.logNodeCopy", 10000);

    private static final Logger LOG = LoggerFactory.getLogger(InitialContentMigrator.class);

    private final NodeStore targetNodeStore;

    private final NodeStore seedNodeStore;

    private final Mount seedMount;

    private final Set<String> includePaths;

    private final Set<String> excludePaths;

    private final Set<String> fragmentPaths;

    private final Set<String> excludeFragments;

    public InitialContentMigrator(NodeStore targetNodeStore, NodeStore seedNodeStore, Mount seedMount) {
        this.targetNodeStore = targetNodeStore;
        this.seedNodeStore = seedNodeStore;
        this.seedMount = seedMount;

        this.includePaths = FilteringNodeState.ALL;
        this.excludeFragments = ImmutableSet.of(seedMount.getPathFragmentName());

        if (seedMount instanceof MountInfo) {
            this.excludePaths = ((MountInfo) seedMount).getIncludedPaths();
            this.fragmentPaths = new HashSet<>();
            for (String p : ((MountInfo) seedMount).getPathsSupportingFragments()) {
                fragmentPaths.add(stripPatternCharacters(p));
            }
        } else {
            this.excludePaths = FilteringNodeState.NONE;
            this.fragmentPaths = FilteringNodeState.ALL;
        }
    }

    private boolean isTargetInitialized() {
        return targetNodeStore.getRoot().hasChildNode("jcr:system");
    }

    public void migrate() throws IOException, CommitFailedException {
        if (isTargetInitialized()) {
            LOG.info("The target is already initialized, no need to copy the seed mount");
        }

        LOG.info("Initializing the default mount using seed {}", seedMount.getName());
        LOG.info("Include: {}", includePaths);
        LOG.info("Exclude: {}", excludePaths);
        LOG.info("Exclude fragments: {} @ {}", excludeFragments, fragmentPaths);

        Map<String, String> nameToRevision = new LinkedHashMap<>();
        Map<String, String> checkpointSegmentToDoc = new LinkedHashMap<>();

        NodeState initialRoot = targetNodeStore.getRoot();
        NodeState targetRoot = initialRoot;
        NodeState previousRoot = initialRoot;
        for (String checkpointName : seedNodeStore.checkpoints()) {
            NodeState checkpointRoot = seedNodeStore.retrieve(checkpointName);
            Map<String, String> checkpointInfo = seedNodeStore.checkpointInfo(checkpointName);

            boolean tracePaths;
            if (previousRoot == initialRoot) {
                LOG.info("Migrating first checkpoint: {}", checkpointName);
                tracePaths = true;
            } else {
                LOG.info("Applying diff to {}", checkpointName);
                tracePaths = false;
            }
            LOG.info("Checkpoint metadata: {}", checkpointInfo);

            targetRoot = copyDiffToTarget(previousRoot, checkpointRoot, targetRoot, tracePaths);
            previousRoot = checkpointRoot;

            String newCheckpointName = targetNodeStore.checkpoint(Long.MAX_VALUE, checkpointInfo);
            if (checkpointInfo.containsKey("name")) {
                nameToRevision.put(checkpointInfo.get("name"), newCheckpointName);
            }
            checkpointSegmentToDoc.put(checkpointName, newCheckpointName);
        }

        NodeState sourceRoot = seedNodeStore.getRoot();
        boolean tracePaths;
        if (previousRoot == initialRoot) {
            LOG.info("No checkpoints found; migrating head");
            tracePaths = true;
        } else {
            LOG.info("Applying diff to head");
            tracePaths = false;
        }

        targetRoot = copyDiffToTarget(previousRoot, sourceRoot, targetRoot, tracePaths);

        LOG.info("Rewriting checkpoint names in /:async {}", nameToRevision);
        NodeBuilder targetBuilder = targetRoot.builder();
        NodeBuilder async = targetBuilder.getChildNode(":async");
        for (Map.Entry<String, String> e : nameToRevision.entrySet()) {
            async.setProperty(e.getKey(), e.getValue(), Type.STRING);

            PropertyState temp = async.getProperty(e.getKey() + "-temp");
            if (temp == null) {
                continue;
            }
            List<String> tempValues = Lists.newArrayList(temp.getValue(Type.STRINGS));
            for (Map.Entry<String, String> sToD : checkpointSegmentToDoc.entrySet()) {
                if (tempValues.contains(sToD.getKey())) {
                    tempValues.set(tempValues.indexOf(sToD.getKey()), sToD.getValue());
                }
            }
            async.setProperty(e.getKey() + "-temp", tempValues, Type.STRINGS);
        }
        targetNodeStore.merge(targetBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private NodeState copyDiffToTarget(NodeState before, NodeState after, NodeState targetRoot, boolean tracePaths) throws IOException, CommitFailedException {
        NodeBuilder targetBuilder = targetRoot.builder();
        NodeState currentRoot = wrapNodeState(after);
        NodeState baseRoot = wrapNodeState(before);
        currentRoot.compareAgainstBaseState(baseRoot, new ApplyDiff(targetBuilder));
        return targetNodeStore.merge(targetBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }


    private NodeState wrapNodeState(NodeState nodeState) {
        NodeState wrapped = nodeState;
        wrapped = FilteringNodeState.wrap("/", wrapped, includePaths, excludePaths, fragmentPaths, excludeFragments);
        wrapped = ReportingNodeState.wrap(wrapped, new LoggingReporter(LOG, "Copying", LOG_NODE_COPY, -1));
        return wrapped;
    }

    private static String stripPatternCharacters(String pathPattern) {
        String result = pathPattern;
        result = substringBefore(result, '*');
        result = substringBefore(result, '$');
        if (!result.equals(pathPattern)) {
            int slashIndex = result.lastIndexOf('/');
            if (slashIndex > 0) {
                result = result.substring(0, slashIndex);
            }
        }
        return result;
    }

    private static String substringBefore(String subject, char stopCharacter) {
        int index = subject.indexOf(stopCharacter);
        if (index > -1) {
            return subject.substring(0, index);
        } else {
            return subject;
        }
    }

}
