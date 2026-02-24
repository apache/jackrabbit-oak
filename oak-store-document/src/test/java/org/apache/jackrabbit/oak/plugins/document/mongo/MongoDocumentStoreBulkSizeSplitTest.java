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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.bson.BsonMaximumSizeExceededException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for bulk operation payload size splitting when combined payload exceeds MongoDB's 16MB limit.
 * Tests cover both the create() and createOrUpdate() methods.
 */
public class MongoDocumentStoreBulkSizeSplitTest extends AbstractMongoConnectionTest {

    private static final int MB = 1024 * 1024;

    private MongoDocumentStore store;

    @Override
    public void setUpConnection() throws Exception {
        mongoConnection = connectionFactory.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDBName());
        DocumentMK.Builder builder = new DocumentMK.Builder();
        store = new MongoDocumentStore(mongoConnection.getMongoClient(),
                mongoConnection.getDatabase(), builder);
        mk = builder.setDocumentStore(store)
                .setMongoDB(mongoConnection.getMongoClient(), mongoConnection.getDBName())
                .open();
    }

    /**
     * Test that create() succeeds when combined payload exceeds 16MB by splitting into smaller batches.
     * Creates multiple documents where combined size > 16MB but individual docs are < 16MB.
     */
    @Test
    public void createWithCombinedPayloadExceeding16MB() {
        // Create documents with ~4MB each, so 5 of them exceed 16MB combined
        int docSize = 4 * MB;
        int numDocs = 5;
        List<UpdateOp> ops = createLargeUpdateOps(numDocs, docSize, "create-split-test");

        // This should succeed after splitting
        boolean result = store.create(Collection.NODES, ops);
        Assert.assertTrue("create() should succeed after splitting large payload", result);

        // Verify all documents were created
        for (int i = 0; i < numDocs; i++) {
            String id = "1:/create-split-test-" + i;
            NodeDocument doc = store.find(Collection.NODES, id);
            Assert.assertNotNull("Document " + id + " should exist", doc);
        }
    }

    /**
     * Test that create() fails when a single document exceeds 16MB.
     * This tests the negative case where splitting cannot help.
     */
    @Test
    public void createWithSingleDocumentExceeding16MB() {
        // Create a single document larger than 16MB
        int docSize = 17 * MB;
        List<UpdateOp> ops = createLargeUpdateOps(1, docSize, "create-single-large");

        // This should fail as single doc exceeds limit
        boolean result = store.create(Collection.NODES, ops);
        Assert.assertFalse("create() should fail when single document exceeds 16MB", result);

        // Verify document was not created
        String id = "1:/create-single-large-0";
        NodeDocument doc = store.find(Collection.NODES, id);
        Assert.assertNull("Document " + id + " should not exist", doc);
        // Document might be null or might exist depending on when exception was thrown
    }

    /**
     * Test that createOrUpdate() succeeds when combined payload exceeds 16MB by splitting.
     * Creates documents first, then updates them with large payloads.
     */
    @Test
    public void createOrUpdateWithCombinedPayloadExceeding16MB() {
        // First create small documents
        int numDocs = 5;
        List<UpdateOp> createOps = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String id = "1:/update-split-test-" + i;
            UpdateOp op = new UpdateOp(id, true);
            op.set("_modified", System.currentTimeMillis() / 1000);
            op.set("initial", "value");
            createOps.add(op);
        }
        Assert.assertTrue("Initial create should succeed", store.create(Collection.NODES, createOps));

        // Now update with large payloads (~4MB each, combined > 16MB)
        int docSize = 4 * MB;
        List<UpdateOp> updateOps = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String id = "1:/update-split-test-" + i;
            UpdateOp op = new UpdateOp(id, false);
            op.set("largeData", generateLargeString(docSize));
            updateOps.add(op);
        }

        // This should succeed after splitting
        List<NodeDocument> results = store.createOrUpdate(Collection.NODES, updateOps);
        Assert.assertNotNull("createOrUpdate() should return results", results);
        Assert.assertEquals("Should have results for all documents", numDocs, results.size());

        // Verify all documents were updated
        for (int i = 0; i < numDocs; i++) {
            String id = "1:/update-split-test-" + i;
            NodeDocument doc = store.find(Collection.NODES, id);
            Assert.assertNotNull("Document " + id + " should exist", doc);
            Assert.assertNotNull("Document should have largeData field", doc.get("largeData"));
        }
    }

    /**
     * Test that createOrUpdate() throws exception when a single document exceeds 16MB.
     * This tests the negative case where splitting cannot help.
     */
    @Test
    public void createOrUpdateWithSingleDocumentExceeding16MB() {
        // First create a small document
        String id = "1:/update-single-large";
        UpdateOp createOp = new UpdateOp(id, true);
        createOp.set("_modified", System.currentTimeMillis() / 1000);
        createOp.set("initial", "value");
        List<UpdateOp> createOps = new ArrayList<>();
        createOps.add(createOp);
        Assert.assertTrue("Initial create should succeed", store.create(Collection.NODES, createOps));

        // Now try to update with a payload larger than 16MB
        int docSize = 17 * MB;
        UpdateOp updateOp = new UpdateOp(id, false);
        updateOp.set("largeData", generateLargeString(docSize));
        List<UpdateOp> updateOps = new ArrayList<>();
        updateOps.add(updateOp);

        // This should throw an exception (DocumentStoreException wrapping BsonMaximumSizeExceededException)
        // Note: With only 1 document, the code takes findAndModify path instead of bulk path
        try {
            store.createOrUpdate(Collection.NODES, updateOps);
            Assert.fail("createOrUpdate() should throw exception when single document exceeds 16MB");
        } catch (DocumentStoreException | BsonMaximumSizeExceededException e) {
            // Expected - DocumentStoreException wraps the BsonMaximumSizeExceededException
            Assert.assertTrue("Exception message should mention size limit",
                    e.getMessage().contains("16793600") || e.getMessage().toLowerCase().contains("size"));
        }
    }

    /**
     * Test mixed scenario: some documents are large but under limit, combined exceeds limit.
     * Verifies that the split logic handles this correctly.
     */
    @Test
    public void createWithMixedSizeDocuments() {
        List<UpdateOp> ops = new ArrayList<>();

        // Add 3 documents of ~6MB each (total ~18MB, exceeds 16MB limit)
        for (int i = 0; i < 3; i++) {
            String id = "1:/mixed-size-" + i;
            UpdateOp op = new UpdateOp(id, true);
            op.set("_modified", System.currentTimeMillis() / 1000);
            op.set("data", generateLargeString(6 * MB));
            ops.add(op);
        }

        // This should succeed after splitting
        boolean result = store.create(Collection.NODES, ops);
        Assert.assertTrue("create() should succeed with mixed size documents after splitting", result);

        // Verify all documents were created
        for (int i = 0; i < 3; i++) {
            String id = "1:/mixed-size-" + i;
            NodeDocument doc = store.find(Collection.NODES, id);
            Assert.assertNotNull("Document " + id + " should exist", doc);
        }
    }

    /**
     * Test that splitting works recursively when first split still exceeds limit.
     * Uses documents sized so that they need multiple levels of splitting.
     */
    @Test
    public void createRequiringMultipleSplits() {
        // Create 8 documents of ~3MB each (total ~24MB)
        // First split: 4+4 = 12MB each batch (still over 16MB? No, 12MB < 16MB)
        // Actually each batch of 4 docs * 3MB = 12MB which is under 16MB
        // Let's use 8 documents of ~5MB each = 40MB total
        // First split: 4*5=20MB per batch, still over 16MB
        // Second split: 2*5=10MB per batch, under 16MB
        int numDocs = 8;
        int docSize = 5 * MB;
        List<UpdateOp> ops = createLargeUpdateOps(numDocs, docSize, "multi-split-test");

        // This should succeed after multiple splits
        boolean result = store.create(Collection.NODES, ops);
        Assert.assertTrue("create() should succeed after multiple splits", result);

        // Verify all documents were created
        for (int i = 0; i < numDocs; i++) {
            String id = "1:/multi-split-test-" + i;
            NodeDocument doc = store.find(Collection.NODES, id);
            Assert.assertNotNull("Document " + id + " should exist", doc);
        }
    }

    /**
     * Test that create() fails when batch contains one document > 16MB mixed with small documents.
     * Even after splitting, the oversized document will fail while small ones may succeed.
     */
    @Test
    public void createWithOneLargeDocumentAmongSmallOnes() {
        List<UpdateOp> ops = new ArrayList<>();

        // Add 5 small documents (~500KB each)
        int smallDocSize = 500 * 1024; // 500KB
        for (int i = 0; i < 5; i++) {
            String id = "1:/mixed-one-large-small-" + i;
            UpdateOp op = new UpdateOp(id, true);
            op.set("_modified", System.currentTimeMillis() / 1000);
            op.set("data", generateLargeString(smallDocSize));
            ops.add(op);
        }

        // Add 1 oversized document (~17MB)
        String largeDocId = "1:/mixed-one-large-big";
        UpdateOp largeOp = new UpdateOp(largeDocId, true);
        largeOp.set("_modified", System.currentTimeMillis() / 1000);
        largeOp.set("data", generateLargeString(17 * MB));
        ops.add(largeOp);

        // This should fail because one document exceeds 16MB
        boolean result = store.create(Collection.NODES, ops);
        Assert.assertFalse("create() should fail when one document exceeds 16MB", result);

        // Small documents may or may not have been created depending on split order,
        // but the large document should definitely not exist
        NodeDocument largeDoc = store.find(Collection.NODES, largeDocId);
        // The large document should not be created
        Assert.assertNull("Document should not exist", largeDoc);
        // Note: depending on implementation, it might be null or the create might have failed entirely
    }

    /**
     * Test that createOrUpdate() throws exception when batch contains one document > 16MB
     * mixed with small documents. The oversized document causes failure after splitting.
     */
    @Test
    public void createOrUpdateWithOneLargeDocumentAmongSmallOnes() {
        // First create all documents with small initial data
        List<UpdateOp> createOps = new ArrayList<>();
        int numSmallDocs = 5;

        for (int i = 0; i < numSmallDocs; i++) {
            String id = "1:/update-mixed-one-large-small-" + i;
            UpdateOp op = new UpdateOp(id, true);
            op.set("_modified", System.currentTimeMillis() / 1000);
            op.set("initial", "small-value");
            createOps.add(op);
        }

        String largeDocId = "1:/update-mixed-one-large-big";
        UpdateOp largeCreateOp = new UpdateOp(largeDocId, true);
        largeCreateOp.set("_modified", System.currentTimeMillis() / 1000);
        largeCreateOp.set("initial", "small-value");
        createOps.add(largeCreateOp);

        Assert.assertTrue("Initial create should succeed", store.create(Collection.NODES, createOps));

        // Now prepare update ops: small updates for small docs, huge update for one doc
        List<UpdateOp> updateOps = new ArrayList<>();

        // Small updates (~500KB each)
        int smallDocSize = 500 * 1024;
        for (int i = 0; i < numSmallDocs; i++) {
            String id = "1:/update-mixed-one-large-small-" + i;
            UpdateOp op = new UpdateOp(id, false);
            op.set("data", generateLargeString(smallDocSize));
            updateOps.add(op);
        }

        // One huge update (~17MB)
        UpdateOp largeUpdateOp = new UpdateOp(largeDocId, false);
        largeUpdateOp.set("data", generateLargeString(17 * MB));
        updateOps.add(largeUpdateOp);

        // This should throw an exception because one doc is too large
        try {
            store.createOrUpdate(Collection.NODES, updateOps);
            Assert.fail("createOrUpdate() should throw exception when one document exceeds 16MB");
        } catch (BsonMaximumSizeExceededException | DocumentStoreException e) {
            // Expected - the oversized document cannot be split further
            Assert.assertTrue("Exception message should mention size",
                    e.getMessage().contains("16793600") || e.getMessage().toLowerCase().contains("size"));
        }

        // Verify the large document was not updated with the huge data
        NodeDocument largeDoc = store.find(Collection.NODES, largeDocId);
        Assert.assertNotNull("Large document should still exist from initial create", largeDoc);
        // The 'data' field should not exist since the update failed
        Object dataField = largeDoc.get("data");
        // data field should be null or not contain the 17MB string
        if (dataField != null) {
            String dataStr = dataField.toString();
            Assert.assertTrue("Large document should not have been updated with 17MB data",
                    dataStr.length() < 17 * MB);
        }
    }

    /**
     * Test create() with 200 documents of 1MB each (200MB total).
     * This requires multiple levels of recursive splitting:
     * - 200 docs × 1MB = 200MB (split to 100+100)
     * - 100 docs × 1MB = 100MB (split to 50+50)
     * - 50 docs × 1MB = 50MB (split to 25+25)
     * - 25 docs × 1MB = 25MB (split to 12+13)
     * - 12/13 docs × 1MB = 12-13MB (under 16MB, success!)
     * Total ~4 levels of recursion needed.
     */
    @Test
    public void createWith200DocumentsRequiringDeepRecursion() {
        int numDocs = 200;
        List<UpdateOp> ops = createLargeUpdateOps(numDocs, MB, "deep-recursion-test");

        // This should succeed after multiple levels of splitting
        boolean result = store.create(Collection.NODES, ops);
        Assert.assertTrue("create() should succeed after deep recursive splitting", result);

        // Verify all 200 documents were created
        for (int i = 0; i < numDocs; i++) {
            String id = "1:/deep-recursion-test-" + i;
            NodeDocument doc = store.find(Collection.NODES, id);
            Assert.assertNotNull("Document " + id + " should exist", doc);
        }
    }

    /**
     * Test create() with 199 documents of 1MB each plus 1 document of 17MB.
     * The splitting will eventually isolate the 17MB document, which will fail.
     * This tests that even with deep recursion, an oversized document is caught.
     */
    @Test
    public void createWith200DocumentsOneOversized() {
        List<UpdateOp> ops = new ArrayList<>();

        // Add 199 documents of 1MB each
        int numSmallDocs = 199;
        String smallData = generateLargeString(MB);
        for (int i = 0; i < numSmallDocs; i++) {
            String id = "1:/deep-recursion-one-large-" + i;
            UpdateOp op = new UpdateOp(id, true);
            op.set("_modified", System.currentTimeMillis() / 1000);
            op.set("data", smallData);
            ops.add(op);
        }

        // Add 1 oversized document of 17MB
        String largeDocId = "1:/deep-recursion-one-large-oversized";
        UpdateOp largeOp = new UpdateOp(largeDocId, true);
        largeOp.set("_modified", System.currentTimeMillis() / 1000);
        largeOp.set("data", generateLargeString(17 * MB));
        ops.add(largeOp);

        // This should fail because one document exceeds 16MB
        boolean result = store.create(Collection.NODES, ops);
        Assert.assertFalse("create() should fail when one document exceeds 16MB even after deep splitting", result);

        // The oversized document should not have been created
        NodeDocument largeDoc = store.find(Collection.NODES, largeDocId);
        // Document should not exist since create failed for it
        Assert.assertNull("Large document should not exist", largeDoc);
    }

    /**
     * Test createOrUpdate() with 200 documents of 1MB each (200MB total).
     * This requires multiple levels of recursive splitting in sendBulkRequest.
     */
    @Test
    public void createOrUpdateWith200DocumentsRequiringDeepRecursion() {
        int numDocs = 200;

        // First create all documents with small initial data
        List<UpdateOp> createOps = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String id = "1:/update-deep-recursion-" + i;
            UpdateOp op = new UpdateOp(id, true);
            op.set("_modified", System.currentTimeMillis() / 1000);
            op.set("initial", "small-value");
            createOps.add(op);
        }
        Assert.assertTrue("Initial create should succeed", store.create(Collection.NODES, createOps));

        // Now update all 200 documents with 1MB data each
        String largeData = generateLargeString(MB);
        List<UpdateOp> updateOps = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String id = "1:/update-deep-recursion-" + i;
            UpdateOp op = new UpdateOp(id, false);
            op.set("data", largeData);
            updateOps.add(op);
        }

        // This should succeed after multiple levels of splitting
        List<NodeDocument> results = store.createOrUpdate(Collection.NODES, updateOps);
        Assert.assertNotNull("createOrUpdate() should return results", results);
        Assert.assertEquals("Should have results for all 200 documents", numDocs, results.size());

        // Verify all documents were updated
        for (int i = 0; i < numDocs; i++) {
            String id = "1:/update-deep-recursion-" + i;
            NodeDocument doc = store.find(Collection.NODES, id);
            Assert.assertNotNull("Document " + id + " should exist", doc);
            Assert.assertNotNull("Document should have data field", doc.get("data"));
        }
    }

    /**
     * Test createOrUpdate() with 199 documents of 1MB each plus 1 document of 17MB.
     * The splitting will eventually isolate the 17MB document, which will fail.
     */
    @Test
    public void createOrUpdateWith200DocumentsOneOversized() {
        int numSmallDocs = 199;

        // First create all 200 documents with small initial data
        List<UpdateOp> createOps = new ArrayList<>();
        for (int i = 0; i < numSmallDocs; i++) {
            String id = "1:/update-deep-one-large-" + i;
            UpdateOp op = new UpdateOp(id, true);
            op.set("_modified", System.currentTimeMillis() / 1000);
            op.set("initial", "small-value");
            createOps.add(op);
        }
        String largeDocId = "1:/update-deep-one-large-oversized";
        UpdateOp largeCreateOp = new UpdateOp(largeDocId, true);
        largeCreateOp.set("_modified", System.currentTimeMillis() / 1000);
        largeCreateOp.set("initial", "small-value");
        createOps.add(largeCreateOp);

        Assert.assertTrue("Initial create should succeed", store.create(Collection.NODES, createOps));

        // Now prepare updates: 199 small (1MB) + 1 oversized (17MB)
        List<UpdateOp> updateOps = new ArrayList<>();
        String smallData = generateLargeString(MB);
        for (int i = 0; i < numSmallDocs; i++) {
            String id = "1:/update-deep-one-large-" + i;
            UpdateOp op = new UpdateOp(id, false);
            op.set("data", smallData);
            updateOps.add(op);
        }

        // One oversized update (17MB)
        UpdateOp largeUpdateOp = new UpdateOp(largeDocId, false);
        largeUpdateOp.set("data", generateLargeString(17 * MB));
        updateOps.add(largeUpdateOp);

        // This should throw an exception because one document exceeds 16MB
        try {
            store.createOrUpdate(Collection.NODES, updateOps);
            Assert.fail("createOrUpdate() should throw exception when one document exceeds 16MB");
        } catch (BsonMaximumSizeExceededException | DocumentStoreException e) {
            // Expected - the oversized document cannot be split further
            Assert.assertTrue("Exception message should mention size",
                    e.getMessage().contains("16793600") || e.getMessage().toLowerCase().contains("size"));
        }

        // The large document should not have been updated with oversized data
        NodeDocument largeDoc = store.find(Collection.NODES, largeDocId);
        Assert.assertNotNull("Large document should still exist from initial create", largeDoc);
        Object dataField = largeDoc.get("data");
        if (dataField != null) {
            String dataStr = dataField.toString();
            Assert.assertTrue("Large document should not have been updated with 17MB data",
                    dataStr.length() < 17 * MB);
        }
    }

    /**
     * Helper method to create UpdateOps with large data.
     */
    private List<UpdateOp> createLargeUpdateOps(int count, int sizePerDoc, String prefix) {
        List<UpdateOp> ops = new ArrayList<>();
        String largeValue = generateLargeString(sizePerDoc);

        for (int i = 0; i < count; i++) {
            String id = "1:/" + prefix + "-" + i;
            UpdateOp op = new UpdateOp(id, true);
            op.set("_modified", System.currentTimeMillis() / 1000);
            op.set("largeData", largeValue);
            ops.add(op);
        }
        return ops;
    }

    /**
     * Helper method to generate a string of approximately the given size.
     */
    private String generateLargeString(int targetSize) {
        StringBuilder sb = new StringBuilder(targetSize);
        // Use a repeating pattern to generate data
        String pattern = "abcdefghijklmnopqrstuvwxyz0123456789";
        while (sb.length() < targetSize) {
            sb.append(pattern);
        }
        return sb.substring(0, targetSize);
    }
}
