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

package org.apache.jackrabbit.oak.segment.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class SafeEncode {

    private SafeEncode() {
        // Prevent instantiation.
    }

    /**
     * Encodes the input string by translating special characters into escape
     * sequences. The resulting string is encoded according to the rules for URL
     * encoding with the exception of the forward slashes and the colon, which
     * are left as-is.
     *
     * @param s A UTF-8 string.
     * @return The encoded string.
     * @throws UnsupportedEncodingException
     */
    public static String safeEncode(String s) throws UnsupportedEncodingException {
        return URLEncoder.encode(s, "UTF-8").replace("%2F", "/").replace("%3A", ":");
    }

}
