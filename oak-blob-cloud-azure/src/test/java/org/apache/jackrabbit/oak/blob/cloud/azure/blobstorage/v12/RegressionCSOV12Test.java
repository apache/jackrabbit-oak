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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v12;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Regression tests for CSO Release 24893 - V12 backend constant and behavior validation.
 * <p>
 * These tests ensure that V12's intentional design choices (larger part sizes, higher URI counts)
 * remain stable and do not regress. The CSO incident highlighted risks when constants change silently;
 * these tests document V12's contract and prevent future refactoring from introducing unexpected changes.
 * <p>
 * Context: V12 uses 256KB minPartSize and 4000MB maxPartSize, intentionally different from V8's
 * 10MB/100MB. This is by design for the V12 SDK. These tests protect that design.
 * <p>
 * Reference: CSO Release 24893 - DAM Archive Download OOM (GRANITE-66069, ASSETS-65164)
 */
public class RegressionCSOV12Test {

    /**
     * V12 MIN_MULTIPART_UPLOAD_PART_SIZE must be 10MB.
     * Originally set to 256KB (V12 SDK default) but caused 40x URI explosion in CSO 24893.
     * For a 10GB download: 256KB → ~41k URIs (~4MB JSON), 10MB → ~1k URIs (~100KB JSON).
     * Fixed to match V8 (OAK-12219 rework). Presigned URI generation now produces sane payloads.
     * Reference: GRANITE-66069, CSO Release 24893
     */
    @Test
    public void v12_minPartSize_mustBe10MB() {
        long expected = 10L * 1024L * 1024L; // 10 MB
        long actual = AzureConstantsV12.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE;

        assertEquals(
                "V12 minPartSize must be 10MB (aligned with V8 for sane URI generation). " +
                        "Originally 256KB in OAK-11267, causing 40x more URIs for large downloads. " +
                        "10GB download: 256KB → ~41k URIs (~4MB JSON); 10MB → ~1k URIs (~100KB JSON). " +
                        "Ref: GRANITE-66069, OAK-12219",
                expected, actual);
    }

    /**
     * V12 MAX_MULTIPART_UPLOAD_PART_SIZE must be 4000MB (4GB).
     * This is the Azure SDK V12 limit for single block uploads.
     * Reducing this would degrade throughput; increasing beyond Azure's limit is invalid.
     */
    @Test
    public void v12_maxPartSize_mustBe4000MB() {
        long expected = 4000L * 1024L * 1024L; // 4000 MB
        long actual = AzureConstantsV12.AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE;

        assertEquals(
                "V12 maxPartSize must be 4000MB (Azure SDK V12 block upload limit). " +
                        "This allows efficient large file uploads via parallel transfer options. " +
                        "Ref: Azure SDK v12 BlockBlobClient limits",
                expected, actual);
    }

    /**
     * V12 MAX_SINGLE_PUT_UPLOAD_SIZE must be 256MB.
     * This is the Azure REST API limit for single PUT operations (non-block uploads).
     * Uploads smaller than this use direct PUT; larger use block commits.
     */
    @Test
    public void v12_maxSinglePutUploadSize_mustBe256MB() {
        long expected = 256L * 1024L * 1024L; // 256 MB
        long actual = AzureConstantsV12.AZURE_BLOB_MAX_SINGLE_PUT_UPLOAD_SIZE;

        assertEquals(
                "V12 maxSinglePutUploadSize must be 256MB (Azure REST API limit). " +
                        "Uploads <= 256MB use direct PUT; larger use block transfer. " +
                        "Ref: Azure Blob Storage REST API Put Blob operation",
                expected, actual);
    }

    /**
     * V12 MAX_BINARY_UPLOAD_SIZE must be ~190.7TiB.
     * This is derived from Azure's 50,000 block limit and 4GB max block size.
     * 50,000 blocks * 4GB/block = 200,000GB ≈ 190.7TiB
     */
    @Test
    public void v12_maxBinaryUploadSize_mustBe190_7TiB() {
        long expected = 190L * 1024L * 1024L * 1024L * 1024L; // ~190.7 TiB
        long actual = AzureConstantsV12.AZURE_BLOB_MAX_BINARY_UPLOAD_SIZE;

        assertEquals(
                "V12 maxBinaryUploadSize must be ~190.7TiB (50k blocks * 4GB max block). " +
                        "Derived from Azure Blob Storage limits. " +
                        "Ref: Azure Blob Storage block limits (50,000 blocks max)",
                expected, actual);
    }

    /**
     * V12 BUFFERED_STREAM_THRESHOLD must be 8MiB.
     * Streams smaller than 8MiB are buffered to memory; larger are buffered to disk.
     * This threshold prevents excessive memory use during large uploads.
     */
    @Test
    public void v12_bufferedStreamThreshold_mustBe8MiB() {
        long expected = 8L * 1024L * 1024L; // 8 MiB
        long actual = AzureConstantsV12.AZURE_BLOB_BUFFERED_STREAM_THRESHOLD;

        assertEquals(
                "V12 bufferedStreamThreshold must be 8MiB. Larger streams use disk buffering. " +
                        "This guards against memory exhaustion during large concurrent uploads. " +
                        "Ref: AzureConstantsV12",
                expected, actual);
    }

    /**
     * V12 MAX_ALLOWABLE_UPLOAD_URIS must be 50,000.
     * This is the Azure Blob Storage hard limit on blocks per blob.
     * Exceeding this causes upload failures.
     */
    @Test
    public void v12_maxAllowableUploadURIs_mustBe50000() {
        int expected = 50000;
        int actual = AzureConstantsV12.AZURE_BLOB_MAX_ALLOWABLE_UPLOAD_URIS;

        assertEquals(
                "V12 maxAllowableUploadURIs must be 50,000 (Azure hard limit on blocks/blob). " +
                        "Presigned URI generation must respect this to prevent upload failures. " +
                        "Ref: Azure Blob Storage limits",
                expected, actual);
    }

    /**
     * V12 DEFAULT_CONCURRENT_REQUEST_COUNT must be 5.
     * This is the default parallelism for multi-part uploads.
     * Tuning this affects throughput vs. memory consumption.
     */
    @Test
    public void v12_defaultConcurrentRequestCount_mustBe5() {
        int expected = 5;
        int actual = AzureConstantsV12.AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT;

        assertEquals(
                "V12 defaultConcurrentRequestCount must be 5 (default parallelism). " +
                        "Affects upload throughput. Changing this impacts performance tuning. " +
                        "Ref: AzureConstantsV12",
                expected, actual);
    }

    /**
     * V12 MAX_CONCURRENT_REQUEST_COUNT must be 10.
     * This is the upper cap on parallelism to prevent overwhelming Azure.
     * Exceeding this can cause throttling or transient failures.
     */
    @Test
    public void v12_maxConcurrentRequestCount_mustBe10() {
        int expected = 10;
        int actual = AzureConstantsV12.AZURE_BLOB_MAX_CONCURRENT_REQUEST_COUNT;

        assertEquals(
                "V12 maxConcurrentRequestCount must be 10 (concurrency cap). " +
                        "Higher values risk Azure throttling. " +
                        "Ref: AzureConstantsV12, Azure rate limiting",
                expected, actual);
    }

    /**
     * V12 PARALLEL_UPLOAD_BLOCK_SIZE must be 4MiB.
     * This is the per-block size used in parallel upload streaming (BlobOutputStream).
     * Larger blocks reduce roundtrips; smaller blocks reduce memory per concurrent block.
     */
    @Test
    public void v12_parallelUploadBlockSize_mustBe4MiB() {
        long expected = 4L * 1024L * 1024L; // 4 MiB
        long actual = AzureConstantsV12.AZURE_BLOB_PARALLEL_UPLOAD_BLOCK_SIZE;

        assertEquals(
                "V12 parallelUploadBlockSize must be 4MiB (per-block size for BlobOutputStream). " +
                        "Tuning this affects upload concurrency and memory footprint. " +
                        "Ref: AzureConstantsV12, Azure SDK v12 BlobOutputStream",
                expected, actual);
    }

    /**
     * V12 PARALLEL_UPLOAD_MAX_CONCURRENCY must be 4.
     * This is the default number of concurrent block uploads for streaming.
     * Higher values increase throughput at cost of memory (4 blocks * 4MiB = 16MiB overhead).
     */
    @Test
    public void v12_parallelUploadMaxConcurrency_mustBe4() {
        int expected = 4;
        int actual = AzureConstantsV12.AZURE_BLOB_PARALLEL_UPLOAD_MAX_CONCURRENCY;

        assertEquals(
                "V12 parallelUploadMaxConcurrency must be 4 (concurrent streaming blocks). " +
                        "Memory overhead: 4 blocks * 4MiB = 16MiB. " +
                        "Ref: AzureConstantsV12, Azure SDK v12 ParallelTransferOptions",
                expected, actual);
    }

    /**
     * Part size ratio test: ensures V12 minPartSize << maxPartSize.
     * Ratio ~400x (4000MB / 10MB). Collapse indicates misconfiguration.
     * maxPartSize is the Azure REST API limit (validator); minPartSize governs URI generation.
     */
    @Test
    public void v12_partSize_ratio_isHealthy() {
        long minSize = AzureConstantsV12.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE;
        long maxSize = AzureConstantsV12.AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE;

        double ratio = (double) maxSize / minSize;
        double expectedRatio = 400.0; // 4000MB / 10MB

        assertEquals(
                "V12 part size ratio must be ~400x (4000MB max / 10MB min). " +
                        "Deviation indicates misconfiguration or refactoring error. " +
                        "Ref: CSO 24893, OAK-12219",
                expectedRatio, ratio, 1.0);
    }

    /**
     * Presigned URI generation scalability: 10GB download with V12's 10MB minPartSize.
     * Expected: ~1,024 URIs (10GB / 10MB) — same as V8, safe for all downstream consumers.
     * Previously 256KB generated ~40,960 URIs (~4MB JSON payload) causing the CSO.
     * Reference: GRANITE-66069, OAK-12219
     */
    @Test
    public void v12_presignedURI_generation_scalability_10GB_download() {
        long minPartSize = AzureConstantsV12.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE;
        long downloadSize = 10L * 1024L * 1024L * 1024L; // 10 GB
        long uriCount = (downloadSize + minPartSize - 1) / minPartSize; // ceiling division

        long expectedURICount = 1024; // 10GB / 10MB
        long actualURICount = uriCount;

        assertEquals(
                "V12 presigned URI count for 10GB download must be ~1,024 (with 10MB minPartSize). " +
                        "Matches V8 behavior. Previously 256KB generated ~40,960 URIs (~4MB JSON). " +
                        "Ref: GRANITE-66069 (CSO 24893), OAK-12219",
                expectedURICount, actualURICount);
    }

    /**
     * Azure's 50,000 block limit caps the maximum addressable size via presigned URIs at minPartSize.
     * Max size = 50,000 blocks * 10MB = ~500 GiB.
     * At the previous 256KB min, this was only 12.5 GiB — files just above that (e.g., the CSO's
     * ~12.8GB test) approached the block limit, generating ~48,805 URIs.
     * At 10MB min, the ceiling is ~500 GiB, well above any realistic single-asset size.
     * Reference: CSO 24893, OAK-12219
     */
    @Test
    public void v12_maxUploadableSize_at_minPartSize_is_500GiB() {
        long minPartSize = AzureConstantsV12.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE; // 10MB
        long maxBlocks = AzureConstantsV12.AZURE_BLOB_MAX_ALLOWABLE_UPLOAD_URIS;        // 50,000
        long maxSize = minPartSize * maxBlocks; // ~500 GiB

        long expected = 10L * 1024L * 1024L * 50_000L;

        assertEquals(
                "Max addressable size via presigned URIs at V12 minPartSize (10MB) is 50,000 * 10MB = ~500 GiB. " +
                        "Well above any realistic single-asset size. " +
                        "Previously 256KB capped this at 12.5 GiB, near the CSO's ~12.8GB test case. " +
                        "Ref: CSO 24893, OAK-12219",
                expected, maxSize);
    }

    /**
     * V12 AZURE_BLOB_UPLOAD_BLOCK_SIZE must be 64MB.
     * This is the block size used for internal Oak uploads via uploadFromFileWithResponse.
     * Previously, uploadBlob() used min(fileSize, MAX_MULTIPART) as blockSize — for a 1GB file
     * that staged a 1GB block; at 5 concurrent uploads = 5GB in-flight memory → OOM.
     * Fixed to a bounded 64MB block size: 5 × 64MB = 320MB max, regardless of file size.
     * Reference: ASSETS-65164, OAK-12219
     */
    @Test
    public void v12_uploadBlockSize_mustBe64MB() {
        long expected = 64L * 1024L * 1024L; // 64 MB
        long actual = AzureConstantsV12.AZURE_BLOB_UPLOAD_BLOCK_SIZE;

        assertEquals(
                "V12 uploadBlockSize must be 64MB (internal block upload granularity). " +
                        "Memory overhead = uploadBlockSize × concurrentRequestCount = 64MB × 5 = 320MB max. " +
                        "Previously used min(fileSize, 4000MB) — 1GB file → 5 × 1GB = 5GB in-flight memory. " +
                        "Ref: ASSETS-65164 (CSO 24893), OAK-12219",
                expected, actual);
    }

    /**
     * Internal upload memory overhead is bounded regardless of file size.
     * Memory = AZURE_BLOB_UPLOAD_BLOCK_SIZE × DEFAULT_CONCURRENT_REQUEST_COUNT = 64MB × 5 = 320MB.
     * Previously: blockSize = min(fileSize, 4000MB) → for a 5GB file, 5 × 4000MB = 20GB in-flight.
     */
    @Test
    public void v12_uploadMemory_bounded_by_blockSize_and_concurrency() {
        long blockSize = AzureConstantsV12.AZURE_BLOB_UPLOAD_BLOCK_SIZE;
        int concurrency = AzureConstantsV12.AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT;
        long maxInFlightMemory = blockSize * concurrency;

        long expectedMax = 64L * 1024L * 1024L * 5; // 320 MB

        assertEquals(
                "Max in-flight upload memory (blockSize × concurrency) must be 320MB (64MB × 5). " +
                        "Bounded regardless of file size. " +
                        "Previously min(fileSize, 4000MB) × 5 could reach 20GB for large files. " +
                        "Ref: ASSETS-65164 (CSO 24893), OAK-12219",
                expectedMax, maxInFlightMemory);
    }

    /**
     * Memory buffering per part: V12's maxPartSize allows up to 4GB buffered per part.
     * This is intentional for V12's higher-throughput design.
     * Consumers (DAM, Archive Download) must stream, not buffer entire parts in memory.
     */
    @Test
    public void v12_memory_buffering_per_part_bounded_at_4GB() {
        long maxPartSize = AzureConstantsV12.AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE;
        long expectedMax = 4000L * 1024L * 1024L; // 4000 MB = 4 GB

        assertEquals(
                "V12 max part size bounds potential memory buffering per part to 4GB. " +
                        "This is acceptable IF downstream consumers stream, not buffer entirely. " +
                        "If a consumer buffers entire parts in memory (like DAM did in CSO), " +
                        "downloading large assets will trigger OOM. " +
                        "Ref: ASSETS-65164 (CSO 24893) - DAM buffered entire parts, causing OOM",
                expectedMax, maxPartSize);
    }

    /**
     * Streaming requirement documentation: V12's large part sizes require streaming consumers.
     * A 4GB part cannot be buffered on typical 4-8GB heaps.
     * This test documents the architectural constraint.
     */
    @Test
    public void v12_requires_streaming_consumers_for_large_parts() {
        long maxPartSize = AzureConstantsV12.AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE; // 4GB
        long typicalHeap = 8L * 1024L * 1024L * 1024L; // 8GB

        assertTrue(
                "V12 part size (4GB) approaches typical heap size (8GB). " +
                        "Buffering entire parts would leave no room for other objects. " +
                        "Downstream consumers MUST stream data, not buffer. " +
                        "This was the root cause of CSO 24893: DAM buffered entire parts. " +
                        "Ref: ASSETS-65164 (CSO 24893)",
                maxPartSize < typicalHeap);
    }
}
