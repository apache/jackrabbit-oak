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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.IOException;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText;
import org.apache.jackrabbit.oak.plugins.index.fulltext.PreExtractedTextProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

class ExtractedTextCache {
    private static final String EMPTY_STRING = "";
    private final Logger log = LoggerFactory.getLogger(getClass());
    private volatile PreExtractedTextProvider extractedTextProvider;
    private int textExtractionCount;
    private long totalBytesRead;
    private long totalTextSize;
    private long totalTime;
    private int preFetchedCount;

    /**
     * Get the pre extracted text for given blob
     * @return null if no pre extracted text entry found. Otherwise returns the pre extracted
     *  text
     */
    @CheckForNull
    public String get(String nodePath, String propertyName, Blob blob, boolean reindexMode){
        String result = null;
        //Consult the PreExtractedTextProvider only in reindex mode and not in
        //incremental indexing mode. As that would only contain older entries
        //That also avoid loading on various state (See DataStoreTextWriter)
        if (reindexMode && extractedTextProvider != null){
            String propertyPath = concat(nodePath, propertyName);
            try {
                ExtractedText text = extractedTextProvider.getText(propertyPath, blob);
                if (text != null) {
                    preFetchedCount++;
                    switch (text.getExtractionResult()) {
                        case SUCCESS:
                            result = text.getExtractedText().toString();
                            break;
                        case ERROR:
                            result = LuceneIndexEditor.TEXT_EXTRACTION_ERROR;
                            break;
                        case EMPTY:
                            result = EMPTY_STRING;
                            break;
                    }
                }
            } catch (IOException e) {
                log.warn("Error occurred while fetching pre extracted text for {}", propertyPath, e);
            }
        }
        return result;
    }

    public void put(Blob blob, ExtractedText extractedText){

    }

    public void addStats(int count, long timeInMillis, long bytesRead, long textLength){
        this.textExtractionCount += count;
        this.totalTime += timeInMillis;
        this.totalBytesRead += bytesRead;
        this.totalTextSize += textLength;
    }

    public TextExtractionStatsMBean getStatsMBean(){
        return new TextExtractionStatsMBean() {
            @Override
            public boolean isPreExtractedTextProviderConfigured() {
                return extractedTextProvider != null;
            }

            @Override
            public int getTextExtractionCount() {
                return textExtractionCount;
            }

            @Override
            public long getTotalTime() {
                return totalTime;
            }

            @Override
            public int getPreFetchedCount() {
                return preFetchedCount;
            }

            @Override
            public String getExtractedTextSize() {
                return IOUtils.humanReadableByteCount(totalTextSize);
            }

            @Override
            public String getBytesRead() {
                return IOUtils.humanReadableByteCount(totalBytesRead);
            }
        };
    }

    public void setExtractedTextProvider(PreExtractedTextProvider extractedTextProvider) {
        this.extractedTextProvider = extractedTextProvider;
    }

    public PreExtractedTextProvider getExtractedTextProvider() {
        return extractedTextProvider;
    }
}
