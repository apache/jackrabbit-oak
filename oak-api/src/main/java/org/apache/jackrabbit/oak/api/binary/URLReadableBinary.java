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

import java.net.URL;
import javax.annotation.Nullable;
import javax.jcr.Binary;
import javax.jcr.RepositoryException;

import org.osgi.annotation.versioning.ProviderType;

/**
 * Optional interface which {@link Binary} values implement that can provide a direct
 * download URL to the underlying blob storage.
 *
 * <h3>Constraints</h3>
 * <p>
 * This direct access in form of a URL will usually be time limited, controlled by the repository,
 * and starting at the time of retrieving the URL. It will only grant access to the particular blob.
 * The client cannot infer any semantics from the URL structure and path names.
 * The URL would typically include a cryptographic signature that covers authentication and expiry time.
 * Any change to the URL will likely result in a failing request.
 * </p>
 *
 * <h3>Usage</h3>
 * <p>
 * Retrieve a binary from a persisted property and check if it provides the URLReadableBinary interface.
 * Then use {@link #getReadURL()} to retrieve the GET URL for downloading the binary contents.
 * </p>
 *
 * Example code:
 * <pre>
 Node file = //... "jcr:content" node beneath a file

 Binary binary = file.getProperty(JcrConstants.JCR_DATA).getBinary();
 if (binary instanceof URLReadableBinary) {
     URL url = ((URLReadableBinary) binary).getReadURL();

     // do HTTP GET on `url` to download the binary contents from the blob storage directly
 } else {
     // not supported, stream normally
     InputStream stream = binary.getInputStream();
 }
 * </pre>
 */
// TODO: should probably move to jackrabbit-api
@ProviderType
public interface URLReadableBinary extends Binary {

    @Nullable
    URL getReadURL() throws RepositoryException;
}
