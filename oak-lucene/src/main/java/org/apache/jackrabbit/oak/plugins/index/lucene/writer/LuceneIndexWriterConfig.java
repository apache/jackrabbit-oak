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

import org.apache.lucene.index.IndexWriterConfig;

public class LuceneIndexWriterConfig {
    /**
     * This property will be used to set Lucene's IndexWriter.maxBufferedDeleteTerms
     * IndexWriter.maxBufferedDeleteTerms is used to flush buffered data to lucene index.
     */
    public final static String MAX_BUFFERED_DELETE_TERMS_KEY = "oak.index.lucene.maxBufferedDeleteTerms";

    /**
     * This property will be used to set Lucene's IndexWriter.perThreadHardLimitMB.
     * IndexWriter.perThreadHardLimitMB is used to flush buffered data to lucene index.
     */
    public final static String RAM_PER_THREAD_HARD_LIMIT_MB_KEY = "oak.index.lucene.ramPerThreadHardLimitMB";

    private final double ramBufferSizeMB;
    private final int maxBufferedDeleteTerms = Integer.getInteger(MAX_BUFFERED_DELETE_TERMS_KEY,
            IndexWriterConfig.DISABLE_AUTO_FLUSH);
    private final int ramPerThreadHardLimitMB = Integer.getInteger(RAM_PER_THREAD_HARD_LIMIT_MB_KEY,
            IndexWriterConfig.DEFAULT_RAM_PER_THREAD_HARD_LIMIT_MB);

    public LuceneIndexWriterConfig() {
        this(IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB);
    }

    public LuceneIndexWriterConfig(double ramBufferSizeMB) {
        this.ramBufferSizeMB = ramBufferSizeMB;
    }

    public double getRamBufferSizeMB() {
        return ramBufferSizeMB;
    }

    public int getMaxBufferedDeleteTerms() {
        return maxBufferedDeleteTerms;
    }

    public int getRamPerThreadHardLimitMB() {
        return ramPerThreadHardLimitMB;
    }
}
