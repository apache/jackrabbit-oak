/**************************************************************************
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
 *
 *************************************************************************/

package org.apache.jackrabbit.oak.api.binary;

import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import org.osgi.annotation.versioning.ProviderType;

/**
 * Optional interface that a {@link javax.jcr.ValueFactory} might implement for a repository
 * that supports direct upload of binaries to the underlying blob storage.
 *
 * <p>
 * More information at {@link URLWritableBinary}.
 * </p>
 */
// TODO: should probably move to jackrabbit-api
@ProviderType
public interface URLWritableBinaryValueFactory {

    /**
     * Creates a new URLWritableBinary for uploading the binary content through a URL instead of
     * an InputStream passed through the JCR API, if supported.
     *
     * A typical use case is if the repository is backed by a binary cloud storage such as S3, where
     * the binary can be uploaded to S3 directly.
     *
     * Note that the write URL of the binary can only be retrieved after it has been set as a binary
     * Property on a Node and after the session has been successfully persisted.
     *
     * If the underlying data store does not support this feature, {@code null}
     * is returned and the binary has to be passed in directly using InputStream as in JCR 2.0.
     *
     * @return a new URLWritableBinary or {@code null} if external binaries are not enabled or supported
     */
    @Nullable
    URLWritableBinary createURLWritableBinary() throws RepositoryException;
}
