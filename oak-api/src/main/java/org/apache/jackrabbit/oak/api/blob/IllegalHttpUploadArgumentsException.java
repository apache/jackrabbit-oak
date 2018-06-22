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
package org.apache.jackrabbit.oak.api.blob;

import javax.jcr.RepositoryException;

/**
 * This exception is thrown from a call to {@link HttpBlobProvider#initiateHttpUpload(long, int)}
 * if no valid upload can be arranged with the arguments specified.  Some examples of
 * how this may occur include:
 *   - If the max upload size is not a positive value
 *   - If the number of URIs requested is zero
 *   - If the number of URIs requested exceeds the maximum allowable value for the
 *     service provider
 *
 * <p>
 * Additionally, the max upload size specified divided by the number of URIs requested
 * indicates the minimum size of each upload.  If that size exceeds the maximum upload
 * size supported by the service provider, this exception is thrown.
 *
 * <p>
 * Each service provider has specific limitations with regard to maximum upload sizes,
 * maximum overall binary sizes, numbers of URIs in multi-part uploads, etc. which
 * can lead to this exception being thrown.  You should consult the documentation for
 * your specific service provider for details.
 *
 * <p>
 * Beyond service provider limitations, the implementation may also choose to enforce
 * its own limitations and may throw this exception based on those limitations.
 * Configuration may also be used to set limitations so this exception may be thrown
 * when configuration parameters are exceeded.
 */
public class IllegalHttpUploadArgumentsException extends RepositoryException {
    public IllegalHttpUploadArgumentsException() {
        super();
    }
    public IllegalHttpUploadArgumentsException(final String message) {
        super(message);
    }
    public IllegalHttpUploadArgumentsException(final Exception e) {
        super(e);
    }
}
