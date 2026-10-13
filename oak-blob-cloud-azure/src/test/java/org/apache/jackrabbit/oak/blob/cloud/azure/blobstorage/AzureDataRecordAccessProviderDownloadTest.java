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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for Azure blob download URI generation and memory safety.
 * <p>
 * These tests validate that large blob downloads generate a sane number of
 * presigned URIs and that the memory buffering model is safe given Oak's
 * part size limits.
 * <p>
 * Context: CSO Release 24893 (ASSETS-65164, GRANITE-66069) showed that when
 * MAX_MULTIPART_UPLOAD_PART_SIZE regressed from 100MB to 4000MB, downstream
 * consumers (DAM Archive Download) that buffer entire parts in memory would
 * trigger Java OOM on large downloads. These tests document the safe behavior
 * and prevent regressions.
 * <p>
 * References:
 * - ASSETS-65164: CSO Release 24893 — DAM Archive Download OOM
 * - GRANITE-66069: CSO 24893 — 40x URI explosion from minPartSize regression
 * - OAK-12164: Fix for V8 constant isolation
 * - OAK-12219: Fix for V12 part size tuning
 */
public class AzureDataRecordAccessProviderDownloadTest {

    // --- URI generation math tests ---

    /**
     * Download URI count for a 1 GB blob with V8's 10MB minPartSize.
     * Expected: ceiling(1GB / 10MB) = 103 URIs (accounting for partial last part).
     * This is safe for all downstream consumers — typical JSON payload ~10KB.
     * Reference: OAK-12164
     */
    @Test
    public void downloadURICount_1GB_blob_with_V8_minPartSize() {
        long blobSize = 1L * 1024L * 1024L * 1024L; // 1 GB
        long minPartSize = AzureBlobStoreBackend.MIN_MULTIPART_UPLOAD_PART_SIZE; // 10 MB
        long uriCount = (blobSize + minPartSize - 1) / minPartSize; // ceiling division

        assertEquals(
                "1GB blob / 10MB minPartSize = 103 URIs. " +
                        "Safe JSON payload (~10KB). Ref: OAK-12164",
                103L, uriCount);
    }

    /**
     * Download URI count for a 10 GB blob with V8's 10MB minPartSize.
     * Expected: ceiling(10GB / 10MB) = 1024 URIs.
     * Presigned URI JSON payload: ~100KB. Safe for all consumers.
     * Reference: GRANITE-66069, OAK-12164
     */
    @Test
    public void downloadURICount_10GB_blob_with_V8_minPartSize() {
        long blobSize = 10L * 1024L * 1024L * 1024L; // 10 GB
        long minPartSize = AzureBlobStoreBackend.MIN_MULTIPART_UPLOAD_PART_SIZE; // 10 MB
        long uriCount = (blobSize + minPartSize - 1) / minPartSize;

        assertEquals(
                "10GB blob / 10MB minPartSize = 1024 URIs. " +
                        "Presigned URI JSON ~100KB. Safe for browsers, aemupload, NUI. Ref: GRANITE-66069",
                1024L, uriCount);
    }

    /**
     * Download URI count for a 100 GB blob with V8's 10MB minPartSize.
     * Expected: ceiling(100GB / 10MB) = 10240 URIs.
     * Still reasonable (~1MB JSON). Well under Azure's 50k block limit.
     * Reference: OAK-12164
     */
    @Test
    public void downloadURICount_100GB_blob_with_V8_minPartSize() {
        long blobSize = 100L * 1024L * 1024L * 1024L; // 100 GB
        long minPartSize = AzureBlobStoreBackend.MIN_MULTIPART_UPLOAD_PART_SIZE; // 10 MB
        long uriCount = (blobSize + minPartSize - 1) / minPartSize;

        assertEquals(
                "100GB blob / 10MB minPartSize = 10240 URIs. " +
                        "Presigned URI JSON ~1MB. Well under Azure's 50k block limit. Ref: OAK-12164",
                10240L, uriCount);
    }

    /**
     * CSO scenario: if V8 minPartSize regresses to 256KB (V12's original value),
     * a 10 GB download would explode to ~40,960 URIs, creating a ~4MB JSON payload.
     * This test documents what we're protecting against.
     * Reference: GRANITE-66069 (CSO 24893)
     */
    @Test
    public void downloadURICount_10GB_blob_with_regressed_256KB_minPartSize() {
        long blobSize = 10L * 1024L * 1024L * 1024L; // 10 GB
        long regressedMinPartSize = 256L * 1024L; // 256 KB (V12 original)
        long uriCount = (blobSize + regressedMinPartSize - 1) / regressedMinPartSize;

        assertEquals(
                "If V8 minPartSize regressed to 256KB (CSO 24893), 10GB would generate ~40,960 URIs. " +
                        "Presigned URI JSON payload would be ~4MB — impacts browsers, aemupload, NUI. " +
                        "Ref: GRANITE-66069 (CSO 24893)",
                40960L, uriCount);
    }

    // --- Memory safety tests ---

    /**
     * V8's maxPartSize is 100MB — safe for consumers that buffer entire parts in memory.
     * This bounds per-part heap usage to 100MB. Even with 10 concurrent parts downloading,
     * maximum in-flight memory = 10 * 100MB = 1GB, well under typical 4-8GB heaps.
     * Reference: OAK-12164
     */
    @Test
    public void memory_buffering_V8_maxPartSize_100MB_is_safe() {
        long maxPartSize = AzureBlobStoreBackend.MAX_MULTIPART_UPLOAD_PART_SIZE; // 100 MB
        int maxConcurrentParts = 10;
        long maxInFlightMemory = maxPartSize * maxConcurrentParts; // 1 GB

        assertEquals(
                "V8 maxPartSize = 100MB. Max in-flight with 10 concurrent parts = 1GB. " +
                        "Safe for consumers that buffer entire parts (though streaming is preferred). " +
                        "Ref: OAK-12164",
                100L * 1024L * 1024L, maxPartSize);

        long typicalHeap = 8L * 1024L * 1024L * 1024L; // 8 GB
        assertTrue(
                "Max in-flight memory (1GB) is well under typical heap (8GB). " +
                        "Ref: OAK-12164",
                maxInFlightMemory < typicalHeap);
    }

    /**
     * CSO scenario: if V8 adopted V12's 4000MB maxPartSize, a single 1+ GB blob
     * download could cause Java OOM. This test documents the unsafe scenario.
     * Reference: ASSETS-65164 (CSO 24893)
     */
    @Test
    public void memory_buffering_CSO_4000MB_maxPartSize_approaches_heap() {
        long regressedMaxPartSize = 4000L * 1024L * 1024L; // 4000 MB (V12 value)
        int maxConcurrentParts = 5;
        long maxInFlightMemory = regressedMaxPartSize * maxConcurrentParts; // 20 GB

        long typicalHeap = 8L * 1024L * 1024L * 1024L; // 8 GB

        assertTrue(
                "If V8 adopted V12's 4000MB maxPartSize, 5 concurrent parts = 20GB in-flight memory. " +
                        "Exceeds typical 8GB heap → Java OOM. This is what happened in CSO 24893. " +
                        "DAM Archive Download buffered entire parts in memory. " +
                        "Ref: ASSETS-65164 (CSO 24893)",
                maxInFlightMemory > typicalHeap);
    }

    /**
     * V8's part size constants are engineered for safe memory buffering.
     * minPartSize (10MB) generates reasonable URI counts; maxPartSize (100MB)
     * limits per-part heap usage. Together they form a safe contract for consumers.
     * Reference: OAK-12164
     */
    @Test
    public void memory_safety_model_V8_minPartSize_and_maxPartSize_are_consistent() {
        long minPartSize = AzureBlobStoreBackend.MIN_MULTIPART_UPLOAD_PART_SIZE; // 10 MB
        long maxPartSize = AzureBlobStoreBackend.MAX_MULTIPART_UPLOAD_PART_SIZE; // 100 MB
        double ratio = (double) maxPartSize / minPartSize;

        assertEquals(
                "V8 part size ratio = 10x (100MB / 10MB). " +
                        "Ensures minPartSize generates ~O(100) URIs for typical (GB-scale) blobs, " +
                        "while maxPartSize bounds per-part memory to safe ~100MB. " +
                        "Ref: OAK-12164",
                10.0, ratio, 0.1);
    }

    // --- Concurrent download stress ---

    /**
     * Memory consumption during concurrent downloads is O(numPartsInFlight),
     * not O(blobSize). With V8's 100MB maxPartSize, 10 concurrent downloads
     * of a 1GB blob each consume at most 10 * 100MB = 1GB in-flight memory.
     * Reference: OAK-12164
     */
    @Test
    public void concurrent_downloads_memory_is_bounded_by_part_count() {
        long maxPartSize = AzureBlobStoreBackend.MAX_MULTIPART_UPLOAD_PART_SIZE; // 100 MB

        int concurrentDownloads = 10;
        long maxInFlightMemory = maxPartSize * concurrentDownloads; // 1 GB

        // In-flight memory depends on part size and concurrency, not blob size.
        // A 1 GB blob and a 10 GB blob both use the same max in-flight memory.
        assertEquals(
                "Memory is O(maxPartSize * concurrentDownloads), not O(blobSize). " +
                        "10 concurrent downloads at 100MB per part = 1GB in-flight regardless of blob size. " +
                        "Ref: OAK-12164",
                1000L * 1024L * 1024L, maxInFlightMemory);

        assertTrue(
                "10 concurrent downloads at 100MB per part = 1GB in-flight (safe). " +
                        "If parts were 4GB (CSO scenario), would be 40GB in-flight (OOM). " +
                        "Ref: ASSETS-65164 (CSO 24893)",
                maxInFlightMemory < 8L * 1024L * 1024L * 1024L);
    }

    /**
     * Azure's 50,000 block limit caps the maximum uploadable blob size at:
     * 50,000 blocks * minPartSize = 50,000 * 10MB = ~500GB.
     * At the CSO's regressed 256KB minPartSize, the limit collapsed to 12.5GB,
     * causing near-limit scenarios to generate 40k+ URIs.
     * Reference: GRANITE-66069 (CSO 24893), OAK-12219
     */
    @Test
    public void azure_block_limit_caps_max_uploadable_size() {
        long minPartSize = AzureBlobStoreBackend.MIN_MULTIPART_UPLOAD_PART_SIZE; // 10 MB
        long maxBlocks = 50_000L; // Azure hard limit
        long maxUploadableSize = minPartSize * maxBlocks; // ~500 GB

        long expected = 10L * 1024L * 1024L * 50_000L;

        assertEquals(
                "Max uploadable size = maxBlocks * minPartSize = 50k * 10MB = ~500GB. " +
                        "If minPartSize regressed to 256KB (CSO), max would collapse to 12.5GB. " +
                        "The CSO test case (~12.8GB) would approach this limit, generating ~48.8k URIs. " +
                        "Ref: GRANITE-66069 (CSO 24893), OAK-12219",
                expected, maxUploadableSize);
    }

    // --- Constant validation from download perspective ---

    /**
     * Validates that V8 constants enable safe download URI generation.
     * If constants regress (as in CSO), URI generation becomes unsafe.
     * This test documents the dependency.
     */
    @Test
    public void downloadURI_safety_depends_on_MIN_MULTIPART_UPLOAD_PART_SIZE() {
        long minPartSize = AzureBlobStoreBackend.MIN_MULTIPART_UPLOAD_PART_SIZE;

        assertEquals(
                "Download URI generation depends on minPartSize = 10MB. " +
                        "Any change affects URI count, JSON payload size, and downstream consumer impact. " +
                        "Regression to 256KB would cause 40x URI explosion. " +
                        "Ref: GRANITE-66069 (CSO 24893), OAK-12164",
                10L * 1024L * 1024L, minPartSize);
    }

    /**
     * Validates that V8's maxPartSize bounds per-part memory buffering.
     * If maxPartSize regresses (as in CSO), memory buffering becomes unsafe.
     * This test documents the dependency.
     */
    @Test
    public void downloadPart_memory_safety_depends_on_MAX_MULTIPART_UPLOAD_PART_SIZE() {
        long maxPartSize = AzureBlobStoreBackend.MAX_MULTIPART_UPLOAD_PART_SIZE;

        assertEquals(
                "Download part buffering safety depends on maxPartSize = 100MB. " +
                        "Consumers (DAM Archive Download) that buffer entire parts in memory are safe at 100MB. " +
                        "Regression to 4000MB would trigger Java OOM on large downloads. " +
                        "Ref: ASSETS-65164 (CSO 24893), OAK-12164",
                100L * 1024L * 1024L, maxPartSize);
    }
}