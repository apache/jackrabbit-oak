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

package org.apache.jackrabbit.oak.plugins.index.fulltext;

import java.io.IOException;

import javax.annotation.CheckForNull;

import org.osgi.annotation.versioning.ConsumerType;
import org.apache.jackrabbit.oak.api.Blob;

@ConsumerType
public interface PreExtractedTextProvider {

    /**
     * Get pre extracted text for given blob at given path
     *
     * @param propertyPath path of the binary property
     * @param blob binary property value
     *
     * @return pre extracted text or null if no
     * pre extracted text found for given blob
     */
    @CheckForNull
    ExtractedText getText(String propertyPath, Blob blob) throws IOException;
}
