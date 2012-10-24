/*
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
 */
package org.apache.jackrabbit.oak.api;

import java.io.IOException;
import java.io.InputStream;

/**
 * BlobFactory...
 * TODO review again if we really need/want to expose that in the OAK API
 * TODO in particular exposing this interface (and Blob) requires additional thoughts on
 * TODO - lifecycle of the factory,
 * TODO - lifecycle of the Blob,
 * TODO - access restrictions and how permissions are enforced on blob creation
 * TODO - searchability, versioning and so forth
 */
public interface BlobFactory {

    /**
     * Create a {@link Blob} from the given input stream. The input stream
     * is closed after this method returns.
     * @param inputStream  The input stream for the {@code Blob}
     * @return  The {@code Blob} representing {@code inputStream}
     * @throws java.io.IOException  If an error occurs while reading from the stream
     */
    Blob createBlob(InputStream inputStream) throws IOException;
}