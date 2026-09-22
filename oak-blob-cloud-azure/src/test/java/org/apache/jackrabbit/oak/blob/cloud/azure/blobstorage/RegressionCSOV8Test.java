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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Regression tests for CSO Release 24893 - V8 backend constant isolation.
 * <p>
 * The CSO was caused by V8 silently adopting V12 constant values during OAK-11267 refactoring.
 * The V8 backend's MIN_MULTIPART_UPLOAD_PART_SIZE changed from 10MB to 256KB and
 * MAX_MULTIPART_UPLOAD_PART_SIZE from 100MB to 4000MB when V8 started importing
 * AzureConstants (V12 values) instead of defining its own.
 * <p>
 * Impact: DAM Archive Download buffers entire binary parts in memory. With max part size
 * now 4GB instead of 100MB, downloading large assets triggered Java OOM, crashing author pods.
 * <p>
 * Fix (OAK-12164): Revert all V8 changes and enforce complete isolation from V12. V8 must
 * define its own constants matching Azure SDK V8 limits, never import from V12.
 * <p>
 * Reference: CSO Release 24893 - DAM Archive Download OOM (GRANITE-66069, ASSETS-65164, OAK-12164)
 */
public class RegressionCSOV8Test {

    // V12 literal values used for isolation assertions — AzureConstantsV12 is package-private
    // in the v12 subpackage and not accessible here. These must match AzureConstantsV12 values.
    private static final long V12_MIN_MULTIPART_UPLOAD_PART_SIZE = 256L * 1024L;           // AzureConstantsV12.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE
    private static final long V12_MAX_MULTIPART_UPLOAD_PART_SIZE = 4000L * 1024L * 1024L;  // AzureConstantsV12.AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE
    private static final long V12_MAX_BINARY_UPLOAD_SIZE = 190L * 1024L * 1024L * 1024L * 1024L; // AzureConstantsV12.AZURE_BLOB_MAX_BINARY_UPLOAD_SIZE

    // --- V8 contract: pin each constant at its correct V8 SDK value ---

    /**
     * V8 MIN_MULTIPART_UPLOAD_PART_SIZE must be 10MB.
     * The CSO regression changed this to 256KB (V12 value), generating 40x more presigned URIs:
     * a 10GB download went from ~1,024 URIs to ~40,960, creating ~4MB JSON payloads with
     * unknown impact on browsers, aemupload, and NUI workers.
     * Ref: GRANITE-66069 (CSO 24893)
     */
    @Test
    public void v8_minPartSize_mustRemain10MB() {
        assertEquals(
                "V8 minPartSize must be 10MB (Azure SDK V8 limit). " +
                        "Regression to 256KB caused 40x URI explosion in CSO 24893. " +
                        "Ref: GRANITE-66069",
                AzureBlobStoreBackend.MIN_MULTIPART_UPLOAD_PART_SIZE, 10L * 1024L * 1024L);
    }

    /**
     * V8 MAX_MULTIPART_UPLOAD_PART_SIZE must be 100MB.
     * The CSO regression changed this to 4000MB (V12 value). DAM Archive Download buffers
     * entire binary parts in memory; with 4GB parts, downloading a 1+ GB JPEG triggered
     * Java OOM, crashing all author pods on release groups 31 and 32.
     * Ref: ASSETS-65164 (CSO 24893)
     */
    @Test
    public void v8_maxPartSize_mustRemain100MB() {
        assertEquals(
                "V8 maxPartSize must be 100MB (Azure SDK V8 limit). " +
                        "Regression to 4000MB caused Java OOM on large DAM downloads in CSO 24893. " +
                        "Ref: ASSETS-65164",
                AzureBlobStoreBackend.MAX_MULTIPART_UPLOAD_PART_SIZE, 100L * 1024L * 1024L);
    }

    /**
     * V8 MAX_SINGLE_PUT_UPLOAD_SIZE must be 256MB.
     * This is the Azure REST API limit for single PUT operations — shared between V8 and V12.
     */
    @Test
    public void v8_maxSinglePutUploadSize_mustBe256MB() {
        assertEquals(
                "V8 maxSinglePutUploadSize must be 256MB (Azure REST API Put Blob limit). " +
                        "Uploads <= 256MB use direct PUT; larger use block transfer. " +
                        "Ref: Azure Blob Storage REST API",
                AzureBlobStoreBackend.MAX_SINGLE_PUT_UPLOAD_SIZE, 256L * 1024L * 1024L);
    }

    /**
     * V8 MAX_BINARY_UPLOAD_SIZE must remain ~4.75TB (Azure SDK V8 limit).
     * The CSO refactoring changed this to V12's ~190.7TiB by importing from AzureConstants.
     * Ref: OAK-12164
     */
    @Test
    public void v8_maxBinaryUploadSize_mustRemain4_75TB() {
        assertEquals(
                "V8 maxBinaryUploadSize must be ~4.75TB (Azure SDK V8 limit). " +
                        "Regression to V12's ~190.7TiB silently altered V8 upload size behavior. " +
                        "Ref: OAK-12164",
                AzureBlobStoreBackend.MAX_BINARY_UPLOAD_SIZE,
                (long) Math.floor(1024L * 1024L * 1024L * 1024L * 4.75));
    }

    // --- Isolation: V8 and V12 constants must not be equal ---

    /**
     * V8 and V12 minPartSize must differ.
     * Equality means V8 is importing V12 constants — the exact refactoring that caused the CSO.
     * Ref: OAK-12164 (fix enforced full code path isolation)
     */
    @Test
    public void v8_minPartSize_mustNotEqualV12() {
        assertNotEquals(
                "V8 and V12 minPartSize must differ. " +
                        "Equality means V8 imported V12 constants — the CSO root cause. " +
                        "V8 must be 10MB; V12 is 256KB. Ref: OAK-12164",
                V12_MIN_MULTIPART_UPLOAD_PART_SIZE,
                AzureBlobStoreBackend.MIN_MULTIPART_UPLOAD_PART_SIZE);
    }

    /**
     * V8 and V12 maxPartSize must differ.
     * Equality means V8 is importing V12 constants — the exact refactoring that caused the CSO.
     * Ref: OAK-12164
     */
    @Test
    public void v8_maxPartSize_mustNotEqualV12() {
        assertNotEquals(
                "V8 and V12 maxPartSize must differ. " +
                        "Equality means V8 imported V12 constants — the CSO root cause. " +
                        "V8 must be 100MB; V12 is 4000MB. Ref: OAK-12164",
                V12_MAX_MULTIPART_UPLOAD_PART_SIZE,
                AzureBlobStoreBackend.MAX_MULTIPART_UPLOAD_PART_SIZE);
    }

    /**
     * V8 and V12 maxBinaryUploadSize must differ.
     * Equality means V8 is importing V12 constants — the exact refactoring that caused the CSO.
     * Ref: OAK-12164
     */
    @Test
    public void v8_maxBinaryUploadSize_mustNotEqualV12() {
        assertNotEquals(
                "V8 and V12 maxBinaryUploadSize must differ. " +
                        "V8 = ~4.75TB (SDK V8 limit), V12 = ~190.7TiB (SDK V12 limit). " +
                        "Equality means V8 imported V12 constants — the CSO root cause. " +
                        "Ref: OAK-12164",
                V12_MAX_BINARY_UPLOAD_SIZE,
                AzureBlobStoreBackend.MAX_BINARY_UPLOAD_SIZE);
    }

    // --- Behavioral impact ---

    /**
     * Part size ratio: V8 maxPartSize / minPartSize must be ~10x (100MB / 10MB).
     * Collapse (e.g., 1:1 or 16000:1) indicates constant sharing or invalid refactoring.
     */
    @Test
    public void v8_partSize_ratio_isHealthy() {
        long min = AzureBlobStoreBackend.MIN_MULTIPART_UPLOAD_PART_SIZE;
        long max = AzureBlobStoreBackend.MAX_MULTIPART_UPLOAD_PART_SIZE;

        assertEquals(
                "V8 part size ratio must be 10x (100MB max / 10MB min). " +
                        "Deviation indicates constant contamination from V12. " +
                        "Ref: CSO 24893",
                10.0, (double) max / min, 0.01);
    }

    /**
     * Presigned URI generation scalability: 10GB download with V8's 10MB minPartSize.
     * Expected ~1,024 URIs. If V8 had adopted V12's 256KB, count would be ~40,960 (40x).
     */
    @Test
    public void v8_presignedURI_generation_scalability_10GB_download() {
        long minPartSize = AzureBlobStoreBackend.MIN_MULTIPART_UPLOAD_PART_SIZE;
        long downloadSize = 10L * 1024L * 1024L * 1024L; // 10 GB
        long uriCount = (downloadSize + minPartSize - 1) / minPartSize;

        assertEquals(
                "V8 presigned URI count for 10GB download must be ~1,024 (with 10MB minPartSize). " +
                        "If V8 had adopted V12's 256KB, count would be ~40,960 — 40x explosion. " +
                        "Ref: GRANITE-66069 (CSO 24893)",
                1024L, uriCount);
    }

    /**
     * Memory buffering per part: V8's 100MB maxPartSize bounds per-part memory to 100MB.
     * Consumers (e.g., DAM Archive Download) that buffer entire parts in memory are safe
     * up to 100MB per part. If V8 had adopted V12's 4000MB, a 1+ GB file would OOM.
     */
    @Test
    public void v8_memory_buffering_per_part_bounded_at_100MB() {
        long maxPartSize = AzureBlobStoreBackend.MAX_MULTIPART_UPLOAD_PART_SIZE;

        assertEquals(
                "V8 maxPartSize bounds per-part memory buffering to 100MB. " +
                        "Safe for consumers that buffer entire parts (e.g., DAM Archive Download). " +
                        "If regressed to V12's 4000MB, 1+ GB downloads cause Java OOM. " +
                        "Ref: ASSETS-65164 (CSO 24893)",
                100L * 1024L * 1024L, maxPartSize);
    }

    /**
     * Heap safety: V8's 100MB maxPartSize is comfortably below typical 4-8GB heaps.
     * Ensures even many concurrent part transfers won't exhaust memory.
     */
    @Test
    public void v8_maxPartSize_safe_relative_to_typical_heap() {
        long maxPartSize = AzureBlobStoreBackend.MAX_MULTIPART_UPLOAD_PART_SIZE;
        long minTypicalHeap = 4L * 1024L * 1024L * 1024L; // 4 GB

        assertTrue(
                "V8 maxPartSize (100MB) must be well under typical heap (4GB). " +
                        "V12's 4000MB part size approaches typical heap, risking OOM under load. " +
                        "Ref: CSO 24893",
                maxPartSize * 10 < minTypicalHeap);
    }
}
