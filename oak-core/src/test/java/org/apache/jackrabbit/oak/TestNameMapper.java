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
package org.apache.jackrabbit.oak;

import java.util.Collections;
import java.util.Map;

import org.apache.jackrabbit.oak.namepath.LocalNameMapper;

/**
 * TestNameMapper... TODO
 */
public final class TestNameMapper extends LocalNameMapper {

    public static final String TEST_LOCAL_PREFIX = "test";
    public static final String TEST_PREFIX = "jr";
    public static final String TEST_URI = "http://jackrabbit.apache.org";

    public static final Map<String, String> LOCAL_MAPPING = Collections.singletonMap(TEST_LOCAL_PREFIX, TEST_URI);

    private final Map<String, String> local;

    public TestNameMapper() {
        this(Collections.singletonMap(TEST_PREFIX, TEST_URI), LOCAL_MAPPING);
    }

    public TestNameMapper(Map<String, String> global, Map<String, String> local) {
        super(global);
        this.local = local;
    }

    @Override
    public Map<String, String> getSessionLocalMappings() {
        return local;
    }
}