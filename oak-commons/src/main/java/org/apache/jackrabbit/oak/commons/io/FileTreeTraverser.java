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
package org.apache.jackrabbit.oak.commons.io;

import org.apache.jackrabbit.oak.commons.Traverser;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FileTreeTraverser {
    private static final @NotNull Function<File, Iterable<? extends File>> FILE_SYSTEM_TREE = new FileSystemTree();

    public static Stream<File> depthFirstPostOrder(File startNode) {
        Iterable<File> iterable = Traverser.postOrderTraversal(startNode, FILE_SYSTEM_TREE);
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    public static Stream<File> breadthFirst(File startNode) {
        Iterable<File> iterable = Traverser.breadthFirstTraversal(startNode, FILE_SYSTEM_TREE);
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    public static Stream<File> depthFirstPreOrder(File startNode) {
        Iterable<File> iterable = Traverser.preOrderTraversal(startNode, FILE_SYSTEM_TREE);
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private static class FileSystemTree implements Function<File, Iterable<? extends File>> {
        @Override
        public @NotNull Iterable<? extends File> apply(File file) {
            if (!file.isDirectory()) {
                return Set.of();
            }
            File[] children = file.listFiles();
            if (children == null || children.length == 0) {
                return Set.of();
            }
            return Set.copyOf(Arrays.asList(children));
        }
    }
}
