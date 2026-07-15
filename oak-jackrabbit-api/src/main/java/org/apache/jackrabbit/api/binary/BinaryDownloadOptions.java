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
package org.apache.jackrabbit.api.binary;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

/**
 * Specifies the options to be used when obtaining a direct download URI via
 * {@link BinaryDownload#getURI(BinaryDownloadOptions)}.  Setting these options
 * allows the caller to instruct the service provider that these options should
 * be applied to the response to a request made with the URI returned.
 * <p>
 * To specify download options, obtain a {@link BinaryDownloadOptionsBuilder}
 * via the {@link #builder()} method, then specify the options desired and
 * get the object via {@link BinaryDownloadOptionsBuilder#build()}.
 * <p>
 * If no options are needed, use {@link BinaryDownloadOptions#DEFAULT} which
 * instructs the implementation to use the service provider default behavior.
 */
@ProviderType
public final class BinaryDownloadOptions {
    private final String mediaType;
    private final String characterEncoding;
    private final String fileName;
    private final String dispositionType;
    private boolean downloadDomainIgnored = false;

    private BinaryDownloadOptions(final String mediaType,
                                  final String characterEncoding,
                                  final String fileName,
                                  final String dispositionType,
                                  boolean downloadDomainIgnored) {
        this.mediaType = mediaType;
        this.characterEncoding = characterEncoding;
        this.fileName = fileName;
        this.dispositionType = dispositionType;
        this.downloadDomainIgnored = downloadDomainIgnored;
    }

    /**
     * Provides a default instance of this class.  Using the default instance
     * indicates that the caller is willing to accept the service provider
     * default behavior.
     */
    public static final BinaryDownloadOptions DEFAULT =
            BinaryDownloadOptions.builder().build();

    /**
     * Returns the internet media type that should be assumed for the binary that is to be
     * downloaded.  This value should be a valid {@code jcr:mimeType}.  This
     * value can be set by calling {@link
     * BinaryDownloadOptionsBuilder#withMediaType(String)} when building an
     * instance of this class.
     *
     * @return A String representation of the internet media type, or {@code null} if no
     *         type has been specified.
     * @see <a href="https://s.apache.org/jcr-2.0-spec/3_Repository_Model.html#3.7.11.10%20mix:mimeType">
     *     JCR 2.0 Repository Model - jcr:mimeType</a>
     */
    @Nullable
    public final String getMediaType() {
        return mediaType;
    }

    /**
     * Returns the character encoding that should be assumed for the binary that
     * is to be downloaded.  This value should be a valid {@code jcr:encoding}.
     * It can be set by calling {@link
     * BinaryDownloadOptionsBuilder#withCharacterEncoding(String)} when building an
     * instance of this class.
     *
     * @return The character encoding, or {@code null} if no
     *         encoding has been specified.
     * @see <a href="https://s.apache.org/jcr-2.0-spec/3_Repository_Model.html#3.7.11.10%20mix:mimeType">
     *     JCR 2.0 Repository Model - jcr:encoding</a>
     */
    @Nullable
    public final String getCharacterEncoding() {
        return characterEncoding;
    }

    /**
     * Returns the filename that should be assumed for the binary that is to be
     * downloaded.  This value can be set by calling {@link
     * BinaryDownloadOptionsBuilder#withFileName(String)} when building an
     * instance of this class.
     *
     * @return The file name, or {@code null} if no
     * file name has been specified.
     */
    @Nullable
    public final String getFileName() {
        return fileName;
    }

    /**
     * Returns the disposition type that should be assumed for the binary that
     * is to be downloaded.  This value can be set by calling {@link
     * BinaryDownloadOptionsBuilder#withDispositionTypeInline()} or {@link
     * BinaryDownloadOptionsBuilder#withDispositionTypeAttachment()} when
     * building an instance of this class.  The default value of this setting is
     * "inline".
     *
     * @return The disposition type.
     * @see <a href="https://tools.ietf.org/html/rfc6266#section-4.2">RFC 6266, Section 4.2</a> 
     */
    @NotNull
    public final String getDispositionType() {
        return dispositionType;
    }

    /**
     * Returns a boolean value that indicates whether the data store should
     * ignore any provided download domain override configuration value when
     * generating the signed URI.  This value can be set by calling {@link
     * BinaryDownloadOptionsBuilder#withDomainOverrideIgnored(boolean)}.  The
     * default value of this setting is false.
     *
     * @return true if the domain override should be ignored; false otherwise.
     */
    public boolean isDownloadDomainIgnored() { return downloadDomainIgnored; }

    /**
     * Returns a {@link BinaryDownloadOptionsBuilder} instance to be used for
     * creating an instance of this class.
     *
     * @return A builder instance.
     */
    @NotNull
    public static BinaryDownloadOptionsBuilder builder() {
        return new BinaryDownloadOptionsBuilder();
    }

    /**
     * Used to build an instance of {@link BinaryDownloadOptions} with the
     * options set as desired by the caller.
     */
    public static final class BinaryDownloadOptionsBuilder {
        private String mediaType = null;
        private String characterEncoding = null;
        private String fileName = null;
        private DispositionType dispositionType = DispositionType.INLINE;
        private boolean domainOverrideIgnored = false;

        private BinaryDownloadOptionsBuilder() { }

        /**
         * Sets the internet media type of the {@link BinaryDownloadOptions} object to be
         * built.  This value should be a valid {@code jcr:mimeType}.
         * <p>
         * Calling this method has the effect of instructing the service
         * provider to set {@code mediaType} as the internet media type
         * in the {@code Content-Type} header field of the response to a request
         * issued with a URI obtained by calling {@link
         * BinaryDownload#getURI(BinaryDownloadOptions)}.  This value can be
         * later retrieved by calling {@link
         * BinaryDownloadOptions#getMediaType()} on the instance returned from a
         * call to {@link #build()}.
         * <p>
         * Note that if the internet media type defines a "charset" parameter
         * (as many textual types do), the caller may also wish to set the
         * character encoding which is done separately.  See {@link
         * #withCharacterEncoding(String)}.
         * <p>
         * The caller should ensure that the internet media type set is valid; the
         * implementation does not perform any validation of this setting.
         * <p>
         * If no internet media type is provided, no {@code Content-Type} header field will be
         * specified to the service provider.
         *
         * @param mediaType The internet media type.
         * @return The calling instance.
         * @see <a href="https://s.apache.org/jcr-2.0-spec/3_Repository_Model.html#3.7.11.10%20mix:mimeType">
         *     JCR 2.0 Repository Model - jcr:mimeType</a>
         */
        @NotNull
        public BinaryDownloadOptionsBuilder withMediaType(@NotNull String mediaType) {
            this.mediaType = mediaType;
            return this;
        }

        /**
         * Sets the character encoding of the {@link BinaryDownloadOptions} object to be
         * built. This value should be a valid {@code jcr:encoding} property value.
         * <p>
         * Calling this method has the effect of instructing the service
         * provider to set {@code characterEncoding} as the "charset" parameter
         * of the content type in the {@code Content-Type} header field of the
         * response to a request issued with a URI obtained by calling {@link
         * BinaryDownload#getURI(BinaryDownloadOptions)}.  This value can be
         * later retrieved by calling {@link
         * BinaryDownloadOptions#getCharacterEncoding()} on the instance returned by a
         * call to {@link #build()}.
         * <p>
         * Note that setting the character encoding only makes sense if the internet
         * media type has also been set, and that media type actually defines a
         * "charset" parameter. See {@link #withMediaType(String)}.
         * <p>
         * The caller should ensure that the proper character encoding has been set for
         * the internet media type; the implementation does not perform any validation of
         * these settings.
         *
         * @param characterEncoding A String representation of the jcr:encoding.
         * @return The calling instance.
         * @see <a href="https://s.apache.org/jcr-2.0-spec/3_Repository_Model.html#3.7.11.10%20mix:mimeType">
         *     JCR 2.0 Repository Model - jcr:encoding</a>
         */
        @NotNull
        public BinaryDownloadOptionsBuilder withCharacterEncoding(@NotNull String characterEncoding) {
            this.characterEncoding = characterEncoding;
            return this;
        }

        /**
         * Sets the filename of the {@link BinaryDownloadOptions} object to be
         * built. This would typically be based on a JCR node name.
         * <p>
         * Calling this method has the effect of instructing the service
         * provider to set {@code fileName} as the filename in the {@code
         * Content-Disposition} header of the response to a request issued with
         * a URI obtained by calling {@link
         * BinaryDownload#getURI(BinaryDownloadOptions)}.  This value can be
         * later retrieved by calling {@link
         * BinaryDownloadOptions#getFileName()} on the instance returned by a
         * call to {@link #build()}.
         * <p>
         *
         * @param fileName The filename.
         * @return The calling instance.
         * @see <a href="https://tools.ietf.org/html/rfc6266#section-4.3">RFC 6266, Section 4.3</a> 
         */
        @NotNull
        public BinaryDownloadOptionsBuilder withFileName(@NotNull String fileName) {
            this.fileName = fileName;
            return this;
        }

        /**
         * Sets the disposition type of the {@link BinaryDownloadOptions} object
         * to be built to {@code inline}.
         * <p>
         * Calling this method has the effect of instructing the service
         * provider to set the disposition type in the {@code
         * Content-Disposition} header of the response to {@code inline}.  This
         * value can be later retrieved by calling {@link
         * BinaryDownloadOptions#getDispositionType()} on the instance built by
         * calling {@link #build()}.
         * <p>
         * If this value is not set, the default value of {@code inline}
         * will be used.
         *
         * @return The calling instance.
         */
        @NotNull
        public BinaryDownloadOptionsBuilder withDispositionTypeInline() {
            dispositionType = DispositionType.INLINE;
            return this;
        }

        /**
         * Sets the disposition type of the {@link BinaryDownloadOptions} object
         * to be built to {@code attachment}.
         * <p>
         * Calling this method has the effect of instructing the service
         * provider to set the disposition type in the {@code
         * Content-Disposition} header of the response to {@code attachment}.
         * This value can later be retrieved by calling {@link
         * BinaryDownloadOptions#getDispositionType()} on the instance built by
         * calling {@link #build()}.
         * <p>
         * If this value is not set, the default value of {@code inline}
         * will be used.
         *
         * @return The calling instance.
         */
        @NotNull
        public BinaryDownloadOptionsBuilder withDispositionTypeAttachment() {
            dispositionType = DispositionType.ATTACHMENT;
            return this;
        }

        /**
         * Sets a flag to indicate whether any configured download domain
         * override value should be ignored when generating the signed download
         * URI.
         * <p>
         *
         * The default value of this flag is false.  This means that if a
         * download domain override configuration value is provided, that value
         * will be used in a signed download URI as the hostname, and if not
         * provided the default hostname will be used instead.  However, if this
         * flag is set to true, the implementation will use the default hostname
         * for the signed download URI regardless of whether the download domain
         * override value is configured or not.
         *
         * Most clients will want to accept the default behavior.  However, if a
         * client understands its deployment topology it may know that ignoring
         * the download domain override will provide better performance.  An
         * example of this is if the client is a service running in the same
         * cloud region as the blob store, in which case accessing the storage
         * directly is almost always faster than going through a domain override
         * (e.g. CDN domain).
         *
         * @param domainOverrideIgnored
         * @return The calling instance.
         */
        public BinaryDownloadOptionsBuilder withDomainOverrideIgnored(boolean domainOverrideIgnored) {
            this.domainOverrideIgnored = domainOverrideIgnored;
            return this;
        }

        /**
         * Construct a {@link BinaryDownloadOptions} instance with the
         * properties specified to the builder.
         *
         * @return A new {@link BinaryDownloadOptions} instance built with the
         *         properties specified to the builder.
         */
        @NotNull
        public BinaryDownloadOptions build() {
            return new BinaryDownloadOptions(mediaType,
                    characterEncoding,
                    fileName,
                    null != dispositionType
                            ? dispositionType.toString()
                            : DispositionType.INLINE.toString(),
                    domainOverrideIgnored
            );
        }

        private enum DispositionType {
            INLINE("inline"),
            ATTACHMENT("attachment");

            private final String value;

            DispositionType(final String value) {
                this.value = value;
            }

            @Override
            public String toString() {
                return value;
            }
        }
    }
}
