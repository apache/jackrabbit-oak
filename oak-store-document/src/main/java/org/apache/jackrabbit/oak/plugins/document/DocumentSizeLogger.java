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
 */
package org.apache.jackrabbit.oak.plugins.document;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for logging large documents when they exceed a configurable threshold.
 * This class provides a NO-OP implementation when the threshold is disabled (0).
 */
public class DocumentSizeLogger {
    
    private static final Logger LOG = LoggerFactory.getLogger(DocumentSizeLogger.class);
    
    /**
     * Enum representing different document operations that can be logged.
     */
    public enum DocumentOperation {
        READ("read"),
        QUERY("query"),
        FIND_DOCUMENTS("findDocuments"),
        FIND_DOCUMENTS_ONE_BY_ONE("findDocumentsOneByOne"),
        PREFETCH("prefetch"),
        CREATE("create"),
        UPDATE("update"),
        UPSERT("upsert"),
        BULK_UPDATE("bulkUpdate");

        private final String operationName;

        DocumentOperation(String operationName) {
            this.operationName = operationName;
        }

        public String getOperationName() {
            return operationName;
        }

        @Override
        public String toString() {
            return operationName;
        }
    }
    
    private final int threshold;
    
    /**
     * Creates a new DocumentSizeLogger with the specified threshold.
     * 
     * @param threshold the size threshold in bytes. If 0, logging is disabled (NO-OP).
     */
    public DocumentSizeLogger(int threshold) {
        this.threshold = threshold;
    }
    
    /**
     * Logs a warning if the document size exceeds the threshold.
     * This is a NO-OP if the threshold is 0 (disabled) or collection is not NODES.
     * 
     * @param document the document to check
     * @param operation the operation being performed
     * @param collection the collection
     */
    public void logIfLarge(Document document, DocumentOperation operation, Collection collection) {
        if (threshold <= 0 || document == null || collection != Collection.NODES) {
            return; // NO-OP if disabled, document is null, or not NODES collection
        }
        
        int size = document.getMemory();
        if (size > threshold) {
            LOG.warn("Large document detected during {} operation: documentId={}, collection={}, size={} bytes, threshold={} bytes", 
                    operation.getOperationName(), document.getId(), collection, size, threshold);
        }
    }
    
    /**
     * Logs a warning if the document size exceeds the threshold.
     * This is a NO-OP if the threshold is 0 (disabled) or collection is not NODES.
     * 
     * @param documentId the document ID
     * @param size the document size in bytes
     * @param operation the operation being performed
     * @param collection the collection
     */
    public void logIfLarge(String documentId, int size, DocumentOperation operation, Collection collection) {
        if (threshold <= 0 || documentId == null || collection != Collection.NODES) {
            return; // NO-OP if disabled, documentId is null, or not NODES collection
        }
        
        if (size > threshold) {
            LOG.warn("Large document detected during {} operation: documentId={}, collection={}, size={} bytes, threshold={} bytes", 
                    operation.getOperationName(), documentId, collection, size, threshold);
        }
    }
    
    /**
     * Returns true if logging is enabled (threshold > 0), false otherwise.
     * 
     * @return true if logging is enabled
     */
    public boolean isEnabled() {
        return threshold > 0;
    }
    
    /**
     * Returns the configured threshold in bytes.
     * 
     * @return the threshold in bytes, or 0 if disabled
     */
    public int getThreshold() {
        return threshold;
    }
}
