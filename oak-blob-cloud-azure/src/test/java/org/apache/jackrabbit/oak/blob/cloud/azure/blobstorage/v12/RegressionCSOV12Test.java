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
     * V12 MIN_MULTIPART_UPLOAD_PART_SIZE must be 256KB.
     * This is intentional for V12 SDK v12 to optimize throughput with larger blocks.
     * Changing this affects URI generation and downstream consumer systems.
     */
    @Test
    public void v12_minPartSize_mustBe256KB() {
        long expected = 256L * 1024L; // 256 KB
        long actual = AzureConstantsV12.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE;

        assertEquals(
                "V12 minPartSize must be 256KB. Changes here affect presigned URI generation " +
                        "and downstream systems (browsers, aemupload, NUI workers). " +
                        "Ref: CSO 24893 - 256KB generates ~40x more URIs than V8's 10MB",
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
     * Ratio ~16000x (4000MB / 256KB) is healthy. Collapse indicates misconfiguration.
     */
    @Test
    public void v12_partSize_ratio_isHealthy() {
        long minSize = AzureConstantsV12.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE;
        long maxSize = AzureConstantsV12.AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE;

        double ratio = (double) maxSize / minSize;
        double expectedRatio = 16000.0; // 4000MB / 256KB

        assertEquals(
                "V12 part size ratio must be ~16000x (4000MB max / 256KB min). " +
                        "Deviation indicates misconfiguration or refactoring error. " +
                        "Ref: CSO 24893",
                expectedRatio, ratio, 1.0);
    }

    /**
     * Presigned URI generation scalability: 10GB download with V12's 256KB minPartSize.
     * Expected: ~40,960 URIs (10GB / 256KB).
     * This documents the URI explosion that motivated the CSO investigation.
     */
    @Test
    public void v12_presignedURI_generation_scalability_10GB_download() {
        long minPartSize = AzureConstantsV12.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE;
        long downloadSize = 10L * 1024L * 1024L * 1024L; // 10 GB
        long uriCount = (downloadSize + minPartSize - 1) / minPartSize; // ceiling division

        long expectedURICount = 40960; // Approximately 10GB / 256KB
        long actualURICount = uriCount;

        assertEquals(
                "V12 presigned URI count for 10GB download is ~40,960 (with 256KB minPartSize). " +
                        "This is 40x more than V8's ~1024 URIs, creating ~4MB JSON payloads. " +
                        "Downstream systems (browsers, aemupload, NUI) must handle this. " +
                        "Ref: GRANITE-66069 (CSO 24893)",
                expectedURICount, actualURICount);
    }

    /**
     * Azure's 50,000 block limit caps the maximum uploadable blob size at current minPartSize.
     * Max size = 50,000 blocks * 256KB = 12.5 GiB.
     * Files larger than this at 256KB min part size cannot be uploaded without increasing the block size.
     * The CSO incident tested a ~12.8GB download which generated ~48,805 URIs — near but under the limit.
     */
    @Test
    public void v12_maxUploadableSize_at_minPartSize_is_12_5GiB() {
        long minPartSize = AzureConstantsV12.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE; // 256KB
        long maxBlocks = AzureConstantsV12.AZURE_BLOB_MAX_ALLOWABLE_UPLOAD_URIS;       // 50,000
        long maxSize = minPartSize * maxBlocks; // 12.5 GiB

        long expected = 256L * 1024L * 50_000L;

        assertEquals(
                "Max uploadable size at V12 minPartSize (256KB) is 50,000 * 256KB = 12.5 GiB. " +
                        "Files larger than this require the SDK to negotiate a larger block size. " +
                        "The CSO tested a ~12.8GB download (~48,805 URIs) approaching this boundary. " +
                        "Ref: CSO 24893 incident report, Azure block limit",
                expected, maxSize);
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
