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

import org.apache.jackrabbit.oak.plugins.document.DocumentSizeLogger.DocumentOperation;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test for DocumentSizeLogger
 */
public class DocumentSizeLoggerTest {

    @Test
    public void testDisabledLogger() {
        DocumentSizeLogger logger = new DocumentSizeLogger(0);
        assertFalse("Logger should be disabled when threshold is 0", logger.isEnabled());
        assertEquals("Threshold should be 0", 0, logger.getThreshold());
        
        // This should be a NO-OP
        logger.logIfLarge(createTestDocument("test"), DocumentOperation.READ, Collection.NODES);
        logger.logIfLarge("test", 1000, DocumentOperation.CREATE, Collection.NODES);
    }

    @Test
    public void testEnabledLogger() {
        DocumentSizeLogger logger = new DocumentSizeLogger(100);
        assertTrue("Logger should be enabled when threshold > 0", logger.isEnabled());
        assertEquals("Threshold should be 100", 100, logger.getThreshold());
    }

    @Test
    public void testLoggingWithNullDocument() {
        DocumentSizeLogger logger = new DocumentSizeLogger(100);
        // This should be a NO-OP
        logger.logIfLarge((Document) null, DocumentOperation.READ, Collection.NODES);
    }

    @Test
    public void testLoggingWithNullDocumentId() {
        DocumentSizeLogger logger = new DocumentSizeLogger(100);
        // This should be a NO-OP
        logger.logIfLarge((String) null, 1000, DocumentOperation.CREATE, Collection.NODES);
    }

    @Test
    public void testLoggingWithNonNodesCollection() {
        DocumentSizeLogger logger = new DocumentSizeLogger(100);
        // This should be a NO-OP for non-NODES collections
        logger.logIfLarge(createTestDocument("test"), DocumentOperation.READ, Collection.CLUSTER_NODES);
        logger.logIfLarge("test", 1000, DocumentOperation.CREATE, Collection.SETTINGS);
    }

    private Document createTestDocument(String id) {
        Document doc = new Document() {
            @Override
            public int getMemory() {
                return 50; // Small document
            }
        };
        doc.put(Document.ID, id);
        return doc;
    }
}
