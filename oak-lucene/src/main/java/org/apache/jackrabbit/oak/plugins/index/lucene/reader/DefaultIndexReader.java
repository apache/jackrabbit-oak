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

package org.apache.jackrabbit.oak.plugins.index.lucene.reader;

import java.io.IOException;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.SuggestHelper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.store.Directory;

import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryUtils.dirSize;

public class DefaultIndexReader implements LuceneIndexReader {
    private final Closer closer;
    private final Directory directory;
    private final Directory suggestDirectory;
    private final IndexReader reader;
    private final AnalyzingInfixSuggester lookup;

    public DefaultIndexReader(Directory directory, @Nullable Directory suggestDirectory, Analyzer analyzer) throws IOException {
        this.closer = Closer.create();
        this.directory = directory;
        closer.register(this.directory);
        this.reader = DirectoryReader.open(directory);
        closer.register(this.reader);
        this.suggestDirectory = suggestDirectory;
        if (suggestDirectory != null) {
            //Directory is closed by AnalyzingInfixSuggester close call
            this.lookup = SuggestHelper.getLookup(suggestDirectory, analyzer);
            closer.register(this.lookup);
        } else {
            this.lookup = null;
        }
    }

    @Override
    public IndexReader getReader() {
        return reader;
    }

    @Override
    @CheckForNull
    public AnalyzingInfixSuggester getLookup() {
        return lookup;
    }

    @Override
    @CheckForNull
    public Directory getSuggestDirectory() {
        return suggestDirectory;
    }

    @Override
    public long getIndexSize() throws IOException {
        return dirSize(directory);
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }
}
