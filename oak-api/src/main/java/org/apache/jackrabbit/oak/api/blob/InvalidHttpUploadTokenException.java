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
 * This exception is thrown from a call to {@link HttpBlobProvider#completeHttpUpload(String)}
 * if the provided {@link String} parameter is found to be an invalid upload token.
 * This occurs if the signature check on the upload token fails or if the token is
 * otherwise unparseable and therefore determined to not be a valid token.
 */
public class InvalidHttpUploadTokenException extends RepositoryException {
    public InvalidHttpUploadTokenException() {
        super();
    }
    public InvalidHttpUploadTokenException(String message) {
        super(message);
    }
    public InvalidHttpUploadTokenException(Exception e) {
        super(e);
    }
}
