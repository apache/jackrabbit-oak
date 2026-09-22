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

import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import org.junit.Test;

import java.time.OffsetDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for BlobSasHeadersV12 — hasHeaders detection, applyTo null-safety, and fluent setters.
 */
public class BlobSasHeadersV12Test {

    @Test
    public void hasHeaders_noFieldsSet_returnsFalse() {
        assertFalse(new BlobSasHeadersV12().hasHeaders());
    }

    @Test
    public void hasHeaders_oneFieldSet_returnsTrue() {
        assertTrue(new BlobSasHeadersV12().setContentType("application/octet-stream").hasHeaders());
    }

    @Test
    public void hasHeaders_allFieldsSet_returnsTrue() {
        assertTrue(new BlobSasHeadersV12("no-cache", "inline", "gzip", "en", "text/plain").hasHeaders());
    }

    /**
     * null sasValues must be a no-op, not a NullPointerException.
     */
    @Test
    public void applyTo_nullSasValues_doesNotThrow() {
        BlobSasHeadersV12 headers = new BlobSasHeadersV12("cc", "cd", "ce", "cl", "ct");
        headers.applyTo(null);
        assertTrue("headers should still report hasHeaders() after a no-op applyTo(null)", headers.hasHeaders());
    }

    /**
     * All five response-header override fields (rscc, rscd, rsce, rscl, rsct) must be wired through to the SAS.
     * Missing any one of them means the browser ignores the override and uses the stored blob metadata instead.
     */
    @Test
    public void applyTo_allFieldsSet_appliesAllToSasValues() {
        BlobSasHeadersV12 headers = new BlobSasHeadersV12("no-cache", "inline", "gzip", "en", "application/json");
        BlobServiceSasSignatureValues sas = new BlobServiceSasSignatureValues(
                OffsetDateTime.now().plusHours(1), BlobSasPermission.parse("r"));

        headers.applyTo(sas);

        assertEquals("no-cache", sas.getCacheControl());
        assertEquals("inline", sas.getContentDisposition());
        assertEquals("gzip", sas.getContentEncoding());
        assertEquals("en", sas.getContentLanguage());
        assertEquals("application/json", sas.getContentType());
    }

    /**
     * Null fields in BlobSasHeadersV12 must not overwrite non-null values already set on the sas object.
     */
    @Test
    public void applyTo_nullFields_doesNotOverrideExistingValues() {
        BlobServiceSasSignatureValues sas = new BlobServiceSasSignatureValues(
                OffsetDateTime.now().plusHours(1), BlobSasPermission.parse("r"));
        sas.setCacheControl("no-store");

        new BlobSasHeadersV12().applyTo(sas);

        assertEquals("no-store", sas.getCacheControl());
    }

    @Test
    public void setters_returnThis_allowsChaining() {
        BlobSasHeadersV12 h = new BlobSasHeadersV12();
        assertSame(h, h.setCacheControl("cc"));
        assertSame(h, h.setContentDisposition("cd"));
        assertSame(h, h.setContentEncoding("ce"));
        assertSame(h, h.setContentLanguage("cl"));
        assertSame(h, h.setContentType("ct"));
    }

    @Test
    public void getters_returnSetValues() {
        BlobSasHeadersV12 h = new BlobSasHeadersV12("cc", "cd", "ce", "cl", "ct");
        assertEquals("cc", h.getCacheControl());
        assertEquals("cd", h.getContentDisposition());
        assertEquals("ce", h.getContentEncoding());
        assertEquals("cl", h.getContentLanguage());
        assertEquals("ct", h.getContentType());
    }
}
