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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

/**
 * A random access trace
 * <p>
 * When {@link Trace#run(NodeState) run} this trace performs random access call to
 * paths passed to its constructor. It logs the current path as additional
 * {@link IOTracer#setContext(List) context}.
 */
public class RandomAccessTrace implements Trace {
    public static final String CONTEXT_SPEC = "path";

    @NotNull
    private final Random rnd;

    private final int count;

    @NotNull
    private final List<String> paths;

    @NotNull
    private final Consumer<List<String>> context;

    /**
     * Create a new instance of a random access trace.
     * @param paths     the list of paths to access
     * @param seed      seed for randomly picking paths
     * @param count     number of paths to trace
     * @param context   consumer to pass the additional context to
     */
    public RandomAccessTrace(@NotNull List<String> paths, long seed, int count, @NotNull Consumer<List<String>> context) {
        this.rnd = new Random(seed);
        this.count = count;
        this.paths = paths;
        this.context = context;
    }

    @Override
    public void run(@NotNull NodeState root) {
        if(!paths.isEmpty()) {
            for (int c = 0; c < count; c++) {
                String path = paths.get(rnd.nextInt(paths.size()));
                context.accept(ImmutableList.of(path));

                NodeState node = root;
                for (String name : elements(getParentPath(path))) {
                    node = node.getChildNode(name);
                }

                String name = getName(path);
                if (node.hasProperty(name)) {
                    PropertyState property = requireNonNull(node.getProperty(name));
                    if (property.isArray()) {
                        for (int k = 0; k < property.count(); k++) {
                            property.getValue(STRING, k);
                        }
                    } else {
                        property.getValue(STRING);
                    }
                } else {
                    node.getChildNode(name);
                }
            }
        }
    }

}
