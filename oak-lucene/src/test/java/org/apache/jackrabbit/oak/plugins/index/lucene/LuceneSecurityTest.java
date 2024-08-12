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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.junit.Test;

/**
 * Tests for potential security issues
 */
public class LuceneSecurityTest {

    @Test
    public void complexRegexp() throws Exception {
        // test borrowed from: https://github.com/apache/lucene/issues/11537
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < 50000; i++) {
            strBuilder.append("a");
        }

        new org.apache.lucene.util.automaton.RegExp(strBuilder.toString());
    }
}
