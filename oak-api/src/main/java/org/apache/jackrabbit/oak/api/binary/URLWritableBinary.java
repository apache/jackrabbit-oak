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
import javax.jcr.Session;

import org.osgi.annotation.versioning.ProviderType;

/**
 * A special {@link Binary} where the client can upload the binary contents to the underlying blob
 * storage directly. A typical example would be a HTTP PUT request to a cloud storage provider
 * such as Amazon S3.
 *
 * <h3>Constraints</h3>
 * <p>
 * This direct access in form of a URL will usually be time limited, controlled by the repository,
 * and starting at the time of the session save. It will only grant access to the particular blob.
 * The repository will be under full control of the name of the blob object, which will be encoded
 * in e.g. the {@link #getWriteURL()}, and might typically be some random UUID. The client cannot
 * infer any semantics from this name. The URL would typically include a cryptographic signature
 * that covers authentication and expiry time. Any change to the URL will likely result in a failing
 * request.
 * </p>
 *
 * <p>
 * Note that an existing Binary value by when reading a JCR property will never support
 * URLWritableBinary. To overwrite such a binary property, clients have to follow the same
 * procedure as creating a new binary property initially: create a new URLWritableBinary via
 * the value factory first, then set this as new binary value on the property, save the session,
 * and finally retrieve the write URL and upload the binary contents.
 * </p>
 *
 * <h3>Usage</h3>
 * <p>
 * This binary can be created using {@link URLWritableBinaryValueFactory}, which is an optional
 * extension interface of the standard {@link javax.jcr.ValueFactory Session.getValueFactory()}.
 * A client has to add this binary to a JCR property first and then persist the session, while
 * keeping the reference to this URLWritableBinary object. After a successful {@link Session#save() save},
 * the client can retrieve e.g. the {@link #getWriteURL() write URL} and use that to upload
 * the binary contents to the blob storage.
 * </p>
 *
 * Example code:
 * <pre>
 Session session = // ... jcr session for writing
 Node file       = //... "jcr:content" node beneath a file

 ValueFactory valueFactory = session.getValueFactory();

 // true for repository implementations that generally support this feature,
 // but it does not mean the feature is enabled for the particular setup
 if (valueFactory instanceof URLWritableBinaryValueFactory) {

     URLWritableBinary binary = ((URLWritableBinaryValueFactory) valueFactory).createURLWritableBinary();
     if (binary == null) {
         // feature not supported, add binary through InputStream
         // for example, leave out the binary property yet, but create the JCR structure
         // then tell the client to upload to an application servlet which then writes the JCR_DATA property using InputStream
     }
     file.setProperty(JcrConstants.JCR_DATA, binary);

     // this will throw if access denied etc.
     session.save();

     // retrieve the URL, only available after the save()
     URL url = binary.getWriteURL();

     // do HTTP PUT on `url` with the binary to upload directly to blob storage...

 } else {
     // handle older JR2/Oak versions here, if needed
 }
 * </pre>
 */
// TODO: should probably move to jackrabbit-api
@ProviderType
public interface URLWritableBinary extends Binary {

    @Nullable
    URL getWriteURL() throws RepositoryException;
}
