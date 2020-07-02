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
package org.apache.jackrabbit.oak.run;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.GenericPropertyState;
import org.apache.jackrabbit.oak.run.Utils.NodeStoreOptions;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.io.Closer;

/**
 * Scans and lists all references to nt:frozenNode and returns an exit code of 1 if any are found (0 otherwise).
 * <p/>
 * This variant uses the /oak:index/reference by scanning through that list (only)
 * and checking if any reference points to an nt:frozenNode (under /jcr:system/jcr:versionStorage
 * at depth > 7).
 * <p/>
 * Example: 
 * <pre>
 * java -mx4g -jar oak-run-*.jar frozennoderefsusingindex mongodb://localhost/&lt;dbname&gt;
 * </pre>
 */
public class FrozenNodeRefsUsingIndexCommand implements Command {

    public static final String NAME = "frozennoderefsusingindex";

    @Override
    public void execute(String... args) throws Exception {
        String help = NAME + " {<path>|<mongo-uri>|<jdbc-uri>} [options]";
        Utils.NodeStoreOptions nopts = new Utils.NodeStoreOptions(help).parse(args);
        if (hasNtFrozenNodeReferences(nopts)) {
            System.err.println("FAILURE: References (in /oak:index/references) to nt:frozenNode found.");
            System.exit(1);
        } else {
            System.out.println("SUCCESS: No references (in /oak:index/references) to nt:frozenNode found.");
        }
    }

    private static boolean hasNtFrozenNodeReferences(NodeStoreOptions nopts) throws IOException {
        Closer closer = Utils.createCloserWithShutdownHook();
        try {
            NodeStore store = Utils.bootstrapNodeStore(nopts, closer);
            NodeState root = store.getRoot();

            NodeBuilder b = root.builder();
            NodeBuilder content = b.child("content");
            PropertyState refProp = GenericPropertyState.referenceProperty("myref", "dd31e965-7e74-4c04-9a64-6698ecb4c624");
            content.setProperty(refProp);
            store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            NodeState oakIndex = root.getChildNode("oak:index");
            NodeState refIndex = oakIndex.getChildNode("reference");
            NodeState uuidIndex = oakIndex.getChildNode("uuid");
            NodeState uuids = uuidIndex.getChildNode(":index");
            System.out.println("Scanning ... refindex = " + refIndex);
            NodeState references = refIndex.getChildNode(":references");

            List<FrozenNodeRef> list = new LinkedList<FrozenNodeRef>();

            for (ChildNodeEntry child : references.getChildNodeEntries()) {
                String uuid = child.getName();
                NodeState uuidRef = uuids.getChildNode(uuid);

                PropertyState entry = uuidRef.getProperty("entry");
                Iterable<String> uuidRefPaths = entry.getValue(Type.STRINGS);
                for (String uuidRefPath : uuidRefPaths) {
                    if (!FrozenNodeRef.isFrozenNodeReferenceCandidate(uuidRefPath)) {
                        continue;
                    }
                    NodeState p = getNodeByPath(root, uuidRefPath);
                    PropertyState primaryType = p.getProperty("jcr:primaryType");
                    boolean isNtFrozenNode = "nt:frozenNode".equals(primaryType.getValue(Type.NAME));
                    if (!isNtFrozenNode) {
                        // this is where we ultimately have to continue out in any case - as only an nt:frozenNode
                        // is what we're interested in.
                        continue;
                    }
                    appendFrozenNodeReference(list, root, child, uuidRefPath, p);
                }
            }
            System.out.println("Scanning done.");

            for (FrozenNodeRef ref : list) {
                System.out.println(FrozenNodeRef.REFERENCE_TO_NT_FROZEN_NODE_FOUND_PREFIX + ref.toInfoString());
            }

            return !list.isEmpty();
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    private static void appendFrozenNodeReference(List<FrozenNodeRef> list, NodeState root, ChildNodeEntry referenceChild, String uuidRefPath,
            NodeState resolvedNodeState) {
        String uuid = referenceChild.getName();
        List<String> paths = new LinkedList<String>();
        scanReferrerPaths(paths, "/", referenceChild.getNodeState());
        for (String referrerPropertyPath : paths) {
            String referrerPath = PathUtils.getAncestorPath(referrerPropertyPath, 1);
            String referrerProperty = PathUtils.getName(referrerPropertyPath);
            FrozenNodeRef ref = new FrozenNodeRef(referrerPath, referrerProperty, "Reference", uuid, uuidRefPath);
            list.add(ref);
        }
    }

    private static void scanReferrerPaths(List<String> paths, String parentPath, NodeState referenceChildNodeState) {
        for (ChildNodeEntry e : referenceChildNodeState.getChildNodeEntries()) {
            String childName = e.getName();
            NodeState childNode = e.getNodeState();
            String childPath = PathUtils.concat(parentPath, childName);
            PropertyState p = childNode.getProperty("match");
            if (p != null && p.getValue(Type.BOOLEAN)) {
                paths.add(childPath);
            }
            scanReferrerPaths(paths, childPath, childNode);
        }
    }

    private static NodeState getNodeByPath(NodeState base, String path) {
        Iterable<String> elems = PathUtils.elements(path);
        NodeState n = base;
        for (String elem : elems) {
            n = n.getChildNode(elem);
        }
        return n;
    }

}
