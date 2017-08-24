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

package org.apache.jackrabbit.oak.plugins.index.lucene.writer;

import java.util.HashMap;
import java.util.Map;


import org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.SuggestHelper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;

public class IndexWriterUtils {

    public static IndexWriterConfig getIndexWriterConfig(IndexDefinition definition, boolean remoteDir) {
        // FIXME: Hack needed to make Lucene work in an OSGi environment
        Thread thread = Thread.currentThread();
        ClassLoader loader = thread.getContextClassLoader();
        thread.setContextClassLoader(IndexWriterConfig.class.getClassLoader());
        try {
            Analyzer definitionAnalyzer = definition.getAnalyzer();
            Map<String, Analyzer> analyzers = new HashMap<String, Analyzer>();
            analyzers.put(FieldNames.SPELLCHECK, new ShingleAnalyzerWrapper(LuceneIndexConstants.ANALYZER, 3));
            if (!definition.isSuggestAnalyzed()) {
                analyzers.put(FieldNames.SUGGEST, SuggestHelper.getAnalyzer());
            }
            Analyzer analyzer = new PerFieldAnalyzerWrapper(definitionAnalyzer, analyzers);
            IndexWriterConfig config = new IndexWriterConfig(VERSION, analyzer);
//            config.setMergePolicy(definition.getMergePolicy());
            if (remoteDir) {
                config.setMergeScheduler(new SerialMergeScheduler());
            }
            if (definition.getCodec() != null) {
                config.setCodec(definition.getCodec());
            }
            return config;
        } finally {
            thread.setContextClassLoader(loader);
        }
    }
}
