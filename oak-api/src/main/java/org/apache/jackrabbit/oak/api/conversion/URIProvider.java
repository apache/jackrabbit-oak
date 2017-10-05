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

package org.apache.jackrabbit.oak.api.conversion;

import org.osgi.annotation.versioning.ProviderType;

import javax.jcr.Value;
import java.net.URI;

/**
 *
 * Provides a URI in exchange for a Value.
 * Typically the Value will represent something where a URI is valuable and useful.
 * Implementations of this interface must ensure that the Oak security model is delegated
 * securely and not circumvented. Only Oak bundles should implement this provider as in most cases
 * internal implementation details of Oak will be required to achieve the implementation. Ideally
 * implementations should be carefully reviewed by peers.
 */
@ProviderType
public interface URIProvider {

    URI toURI(Value value);
}
