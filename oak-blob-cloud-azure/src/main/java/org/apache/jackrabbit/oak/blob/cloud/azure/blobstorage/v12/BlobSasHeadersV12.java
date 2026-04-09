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

import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import org.jetbrains.annotations.Nullable;

/**
 * Represents the optional headers that can be returned using SAS (Shared Access Signature).
 * This class is the Azure SDK 12 equivalent of the legacy V8 {@code SharedAccessBlobHeaders}.
 * 
 * <p>These headers are set on the {@link BlobServiceSasSignatureValues} object and will be
 * returned to the client when the SAS token is used to access the blob.</p>
 * 
 * @see <a href="https://learn.microsoft.com/en-us/java/api/com.azure.storage.blob.sas.blobservicesassignaturevalues">BlobServiceSasSignatureValues</a>
 */
public class BlobSasHeadersV12 {
    
    private String cacheControl;
    private String contentDisposition;
    private String contentEncoding;
    private String contentLanguage;
    private String contentType;

    /**
     * Creates an empty BlobSasHeadersV12 object.
     */
    public BlobSasHeadersV12() {
    }

    /**
     * Creates a BlobSasHeadersV12 object with the specified values.
     *
     * @param cacheControl the cache-control header value
     * @param contentDisposition the content-disposition header value
     * @param contentEncoding the content-encoding header value
     * @param contentLanguage the content-language header value
     * @param contentType the content-type header value
     */
    public BlobSasHeadersV12(@Nullable String cacheControl,
                          @Nullable String contentDisposition,
                          @Nullable String contentEncoding,
                          @Nullable String contentLanguage,
                          @Nullable String contentType) {
        this.cacheControl = cacheControl;
        this.contentDisposition = contentDisposition;
        this.contentEncoding = contentEncoding;
        this.contentLanguage = contentLanguage;
        this.contentType = contentType;
    }

    /**
     * Gets the cache-control header value.
     *
     * @return the cache-control header value
     */
    @Nullable
    public String getCacheControl() {
        return cacheControl;
    }

    /**
     * Sets the cache-control header value.
     *
     * @param cacheControl the cache-control header value
     * @return this BlobSasHeadersV12 object for method chaining
     */
    public BlobSasHeadersV12 setCacheControl(@Nullable String cacheControl) {
        this.cacheControl = cacheControl;
        return this;
    }

    /**
     * Gets the content-disposition header value.
     *
     * @return the content-disposition header value
     */
    @Nullable
    public String getContentDisposition() {
        return contentDisposition;
    }

    /**
     * Sets the content-disposition header value.
     *
     * @param contentDisposition the content-disposition header value
     * @return this BlobSasHeadersV12 object for method chaining
     */
    public BlobSasHeadersV12 setContentDisposition(@Nullable String contentDisposition) {
        this.contentDisposition = contentDisposition;
        return this;
    }

    /**
     * Gets the content-encoding header value.
     *
     * @return the content-encoding header value
     */
    @Nullable
    public String getContentEncoding() {
        return contentEncoding;
    }

    /**
     * Sets the content-encoding header value.
     *
     * @param contentEncoding the content-encoding header value
     * @return this BlobSasHeadersV12 object for method chaining
     */
    public BlobSasHeadersV12 setContentEncoding(@Nullable String contentEncoding) {
        this.contentEncoding = contentEncoding;
        return this;
    }

    /**
     * Gets the content-language header value.
     *
     * @return the content-language header value
     */
    @Nullable
    public String getContentLanguage() {
        return contentLanguage;
    }

    /**
     * Sets the content-language header value.
     *
     * @param contentLanguage the content-language header value
     * @return this BlobSasHeadersV12 object for method chaining
     */
    public BlobSasHeadersV12 setContentLanguage(@Nullable String contentLanguage) {
        this.contentLanguage = contentLanguage;
        return this;
    }

    /**
     * Gets the content-type header value.
     *
     * @return the content-type header value
     */
    @Nullable
    public String getContentType() {
        return contentType;
    }

    /**
     * Sets the content-type header value.
     *
     * @param contentType the content-type header value
     * @return this BlobSasHeadersV12 object for method chaining
     */
    public BlobSasHeadersV12 setContentType(@Nullable String contentType) {
        this.contentType = contentType;
        return this;
    }

    /**
     * Applies these headers to the given {@link BlobServiceSasSignatureValues} object.
     * Only non-null headers are set.
     *
     * @param sasSignatureValues the BlobServiceSasSignatureValues object to apply headers to
     */
    public void applyTo(BlobServiceSasSignatureValues sasSignatureValues) {
        if (sasSignatureValues == null) {
            return;
        }
        
        if (cacheControl != null) {
            sasSignatureValues.setCacheControl(cacheControl);
        }
        if (contentDisposition != null) {
            sasSignatureValues.setContentDisposition(contentDisposition);
        }
        if (contentEncoding != null) {
            sasSignatureValues.setContentEncoding(contentEncoding);
        }
        if (contentLanguage != null) {
            sasSignatureValues.setContentLanguage(contentLanguage);
        }
        if (contentType != null) {
            sasSignatureValues.setContentType(contentType);
        }
    }

    /**
     * Checks if any headers are set (non-null).
     *
     * @return true if at least one header is set, false otherwise
     */
    public boolean hasHeaders() {
        return cacheControl != null || contentDisposition != null || contentEncoding != null 
                || contentLanguage != null || contentType != null;
    }
}

