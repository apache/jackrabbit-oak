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

package org.apache.jackrabbit.oak.plugins.index.search;

import org.osgi.annotation.versioning.ProviderType;

/**
 * An MBean for text extraction statistics.
 */
@ProviderType
public interface TextExtractionStatsMBean {

    /**
     * Type of this MBean
     */
    String TYPE = "TextExtractionStats";

    /**
     * Check whether pre extracted text provider is configured
     * @return {@code true} if configured, {@code false} otherwise
     */
    boolean isPreExtractedTextProviderConfigured();

    /**
     * Check whether pre extracted cache should always be used
     * @return {@code true} if PEC should always be used, {@code false} otherwise
     */
    boolean isAlwaysUsePreExtractedCache();

    /**
     * Number of text extraction operations performed
     * @return the text extraction count
     */
    int getTextExtractionCount();

    /**
     * Total time taken by text extraction
     * @return total time taken
     */
    long getTotalTime();

    /**
     * Pre fetch count
     * @return no. of prefetch operations
     */
    int getPreFetchedCount();

    /**
     * Size of extracted size
     * @return extracted text size
     */
    String getExtractedTextSize();

    /**
     * Bytes read by text extraction
     * @return bytes read
     */
    String getBytesRead();

    /**
     * Count of extractions gone timeout
     * @return timeout count
     */
    int getTimeoutCount();
}
