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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * {@link IndexHook} implementation that is responsible for keeping the
 * {@link LuceneIndex} up to date
 * 
 * @see LuceneIndex
 * 
 */
public class LuceneHook implements IndexHook {

    private final NodeBuilder builder;

    private IndexHook luceneEditor;

    public LuceneHook(NodeBuilder builder) {
        this.builder = builder;
    }

    // -----------------------------------------------------< IndexHook >--

    @Override
    @Nonnull
    public NodeStateDiff preProcess() throws CommitFailedException {
        luceneEditor = new LuceneEditor(builder);
        return luceneEditor.preProcess();
    }

    @Override
    public void postProcess() throws CommitFailedException {
        luceneEditor.postProcess();
    }

    @Override
    public void close() throws IOException {
        try {
            if (luceneEditor != null) {
                luceneEditor.close();
            }
        } finally {
            luceneEditor = null;
        }
    }
}
