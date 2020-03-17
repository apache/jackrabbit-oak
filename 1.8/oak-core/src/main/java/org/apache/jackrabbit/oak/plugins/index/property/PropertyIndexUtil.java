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

package org.apache.jackrabbit.oak.plugins.index.property;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Charsets;
import org.apache.jackrabbit.oak.api.PropertyValue;

public class PropertyIndexUtil {
    // TODO the max string length should be removed, or made configurable
    private static final int MAX_STRING_LENGTH = 100;

    /**
     * name used when the indexed value is an empty string
     */
    private static final String EMPTY_TOKEN = ":";

    public static Set<String> encode(PropertyValue value, ValuePattern pattern) {
        return encode(ValuePatternUtil.read(value, pattern));
    }

    public static Set<String> encode(Set<String> set) {
        if (set == null || set.isEmpty()) {
            return set;
        }
        try {
            Set<String> values = new HashSet<String>();
            for(String v : set) {
                if (v.length() > MAX_STRING_LENGTH) {
                    v = v.substring(0, MAX_STRING_LENGTH);
                }
                if (v.isEmpty()) {
                    v = EMPTY_TOKEN;
                } else {
                    v = URLEncoder.encode(v, Charsets.UTF_8.name());
                }
                values.add(v);
            }
            return values;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 is unsupported", e);
        }
    }
}
