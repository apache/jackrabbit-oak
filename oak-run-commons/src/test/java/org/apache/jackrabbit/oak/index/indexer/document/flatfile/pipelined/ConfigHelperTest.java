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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ConfigHelperTest {

    @Rule
    public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

    @Test
    public void getSystemPropertyAsStringList() {
        assertEquals(List.of(), ConfigHelper.getSystemPropertyAsStringList("not.defined", "", ';'));
        assertEquals(List.of("default"), ConfigHelper.getSystemPropertyAsStringList("not.defined", "default", ';'));
        assertEquals(List.of("default1", "default2"), ConfigHelper.getSystemPropertyAsStringList("not.defined", "default1;default2", ';'));

        test(List.of("value1"), "value1", "default", ';');
        test(List.of(), " ", "default", ';');
        test(List.of("v1", "v2"), "v1;v2", "default", ';');
        test(List.of("v1", "v2"), "v1; v2", "default", ';');
        test(List.of("v1", "v2"), "v1,v2", "default", ',');
        test(List.of("v1,v2"), "v1,v2", "default", ';');
    }

    private void test(List<String> expected, String sysPropertyValue, String defaultValue, char separator) {
        String key = "$$ConfigHelperTest_KEY$$";
        System.setProperty(key, sysPropertyValue);
        try {
            assertEquals(expected, ConfigHelper.getSystemPropertyAsStringList(key, defaultValue, separator));
        } finally {
            System.clearProperty(key);
        }
    }
}