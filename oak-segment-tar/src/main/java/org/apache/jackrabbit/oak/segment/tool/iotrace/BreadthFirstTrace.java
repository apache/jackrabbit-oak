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

package org.apache.jackrabbit.oak.segment.tool.iotrace;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newLinkedList;
import static java.lang.String.valueOf;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

import java.io.Writer;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A breadth first traversal trace.
 * <p>
 * When {@link Trace#run(NodeState) run} this trace performs a breadth first traversal starting
 * from the passed node down to a certain depth. It logs the current depth and the number of
 * traversed nodes as additional {@link IOTracer#setContext(List) context}.
 */
public class BreadthFirstTrace implements Trace {

    /**
     * The context specification of this trace.
     * @see IOTracer#newIOTracer(Function, Writer, String)
     */
    @Nonnull
    public static final String CONTEXT_SPEC = "depth,count";

    private final int depth;

    @Nonnull
    private final String path;

    @Nonnull
    private final Consumer<List<String>> context;

    @Nonnull
    private final AtomicInteger nodeCount = new AtomicInteger();

    /**
     * Create a new instance of a breadth first traversal trace.
     * @param depth     maximal depth of the nodes to traverse
     * @param path      path of the root node where to start traversing
     * @param context   consumer to pass the additional context to
     */
    public BreadthFirstTrace(int depth, @Nonnull String path, @Nonnull Consumer<List<String>> context) {
        checkArgument(depth >= 0);

        this.depth = depth;
        this.path = path;
        this.context = context;
    }

    @Override
    public void run(@Nonnull NodeState node) {
        updateContext(context, 0, nodeCount.incrementAndGet());
        traverse(newLinkedList(singleton(getNode(node, path))), 0);
    }

    @Nonnull
    private static NodeState getNode(@Nonnull NodeState root, @Nonnull String path) {
        NodeState node = root;
        for (String name : elements(path)) {
            node = node.getChildNode(name);
        }
        return node;
    }

    private void traverse(@Nonnull Queue<NodeState> nodes, int depth) {
        if (!nodes.isEmpty()) {
            Queue<NodeState> children = newLinkedList();
            while (!nodes.isEmpty()) {
                NodeState head = nodes.poll();
                assert head != null;
                if (depth < this.depth) {
                    head.getChildNodeEntries().forEach(
                        cse -> {
                            updateContext(context, depth + 1, nodeCount.incrementAndGet());
                            NodeState child = cse.getNodeState();
                            if (depth + 1 < this.depth) {
                                // Only add to children queue if not at last level to save memory
                                children.offer(child);
                            }
                        });
                }
            }
            traverse(children, depth + 1);
        }
    }

    private static void updateContext(@Nonnull Consumer<List<String>> context, int depth, int count) {
        context.accept(ImmutableList.of(valueOf(depth), valueOf(count)));
    }

}
