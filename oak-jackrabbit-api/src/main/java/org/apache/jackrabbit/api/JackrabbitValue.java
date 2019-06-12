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
package org.apache.jackrabbit.api;

import javax.jcr.Value;

/**
 * Values returned by Jackrabbit may implement this interface. The interface
 * defines optional features. An application should check if the returned value
 * is of this type before casting as in:
 * <pre>
 * if (v instanceof JackrabbitValue) {
 *     JackrabbitValue j = (JackrabbitValue) v;
 *     ....
 * }
 * </pre>
 */
public interface JackrabbitValue extends Value {

    /**
     * Get a unique identifier of the content of this value. Usually this is a
     * message digest of the content (a cryptographically secure one-way hash).
     * This allows to avoid processing large binary values multiple times.
     * <p>
     * This method returns null if the identifier is unknown. The identifier may
     * not always be available, for example if the value has not yet been saved
     * or processed. Once an identifier is available, it will never change
     * because values are immutable.
     * <p>
     * If two values have the same identifier, the content of the value is
     * guaranteed to be the same. However it is not guaranteed that two values
     * with the same content will return the same identifier.
     * <p>
     * The identifier is opaque, meaning it can have any format and size, however
     * it is at normally about 50 characters and at most 255 characters long.
     * The string only contains Unicode code points from 32 to 127 (including).
     *
     * @return the unique identifier or null
     */
    String getContentIdentity();

}
