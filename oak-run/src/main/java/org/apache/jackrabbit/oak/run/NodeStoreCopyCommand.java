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
package org.apache.jackrabbit.oak.run;

import java.util.ArrayList;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import joptsimple.OptionParser;

public class NodeStoreCopyCommand implements Command {
    
    public static final String NAME = "nodestore-copy";

    @Override
    public void execute(String... args) throws Exception {
        
        ArrayList<String> sourceArgs = new ArrayList<>();
        ArrayList<String> targetArgs = new ArrayList<>();
        for(String a: args) {
            if (a.startsWith("s:")) {
                sourceArgs.add(a.substring(2));
            } else if (a.startsWith("t:")) {
                targetArgs.add(a.substring(2));
            } else {
                printHelp();
                return;
            }
        }
        
        OptionParser sourceParser = new OptionParser();
        OptionParser targetParser = new OptionParser();
        Options sourceOpts = new Options();
        Options targetOpts = new Options();
        sourceOpts.parseAndConfigure(sourceParser, sourceArgs.toArray(new String[0]));
        targetOpts.parseAndConfigure(targetParser, targetArgs.toArray(new String[0]));

        try (NodeStoreFixture sourceFixture = NodeStoreFixtureProvider.create(sourceOpts)) {
            NodeStore sourceNs = sourceFixture.getStore();
            try (NodeStoreFixture targetFixture = NodeStoreFixtureProvider.create(targetOpts)) {
                NodeStore targetNs = targetFixture.getStore();
                copy(sourceNs, targetNs);
            }
        }
    }

    private void copy(NodeStore sourceNs, NodeStore targetNs) throws CommitFailedException {
        NodeBuilder b = targetNs.getRoot().builder();
        NodeState n = sourceNs.getRoot();
        copy(n, b, 0);
        targetNs.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        targetNs.checkpoint(1000 * 60 * 60 * 12);
    }

    private void copy(NodeState source, NodeBuilder target, int count) {
        for(PropertyState p : source.getProperties()) {
            target.setProperty(p);
        }
        count++;
        for (ChildNodeEntry c : source.getChildNodeEntries()) {
            NodeBuilder cb = target.child(c.getName());
            copy(c.getNodeState(), cb, count++);
        }
    }

    private void printHelp() {
        System.out.println("Copies all nodes from the source to the target nodestore");
        System.out.println("Usage: ");
        System.out.println("  Source node store is configured using arguments starting with 's:'");
        System.out.println("  Target node store is configured using arguments starting with 't:'");
    }

}