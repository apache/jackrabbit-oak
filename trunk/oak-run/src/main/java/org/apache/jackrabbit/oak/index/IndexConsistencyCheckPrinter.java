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

package org.apache.jackrabbit.oak.index;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import org.apache.commons.io.output.WriterOutputStream;
import org.apache.felix.inventory.Format;
import org.apache.felix.inventory.InventoryPrinter;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexConsistencyChecker;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexConsistencyChecker.Level;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;

class IndexConsistencyCheckPrinter implements InventoryPrinter {
    private final IndexHelper indexHelper;
    private final Level level;

    public IndexConsistencyCheckPrinter(IndexHelper indexHelper, int level) {
        this.indexHelper = indexHelper;
        this.level = level == 1 ? Level.BLOBS_ONLY : Level.FULL;
    }

    @Override
    public void print(PrintWriter pw, Format format, boolean isZip) {
        Stopwatch watch = Stopwatch.createStarted();
        NodeState root = indexHelper.getNodeStore().getRoot();

        List<String> validIndexes = new ArrayList<>();
        List<String> invalidIndexes = new ArrayList<>();
        List<String> ignoredIndexes = new ArrayList<>();

        for (String indexPath : indexHelper.getIndexPathService().getIndexPaths()) {
            NodeState indexState = NodeStateUtils.getNode(root, indexPath);
            if (!TYPE_LUCENE.equals(indexState.getString(TYPE_PROPERTY_NAME))){
                ignoredIndexes.add(indexPath);
                continue;
            }

            IndexConsistencyChecker checker = new IndexConsistencyChecker(root, indexPath, indexHelper.getWorkDir());
            checker.setPrintStream(new PrintStream(new WriterOutputStream(pw, Charsets.UTF_8)));
            try {
                IndexConsistencyChecker.Result result = checker.check(level);
                result.dump(pw);
                if (result.clean) {
                    validIndexes.add(indexPath);
                } else {
                    invalidIndexes.add(indexPath);
                }
                System.out.printf("%s => %s%n", indexPath, result.clean ? "valid" : "invalid <==");
            } catch (Exception e) {
                invalidIndexes.add(indexPath);
                pw.printf("Error occurred while performing consistency check for index [%s]%n", indexPath);
                e.printStackTrace(pw);
            }
            pw.println();
        }

        print(validIndexes, "Valid indexes :", pw);
        print(invalidIndexes, "Invalid indexes :", pw);
        print(ignoredIndexes, "Ignored indexes as these are not of type lucene:", pw);
        pw.printf("Time taken %s%n", watch);
    }

    private static void print(List<String> indexPaths, String message, PrintWriter pw) {
        if (!indexPaths.isEmpty()) {
            pw.println(message);
            indexPaths.forEach((path) -> pw.printf("    - %s%n", path));
        }
    }
}
