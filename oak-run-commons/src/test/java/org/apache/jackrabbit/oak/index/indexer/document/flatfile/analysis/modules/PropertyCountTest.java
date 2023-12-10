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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules;

import java.util.Arrays;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Storage;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property.ValueType;
import org.junit.Test;

public class PropertyCountTest {
    
    @Test
    public void manyUniqueProperties() {
        PropertyStats pc = new PropertyStats();
        pc.setStorage(new Storage());
        for (int i = 0; i < 1_000_000; i++) {
            Property p = new Property("unique" + i, ValueType.STRING, "");
            NodeData n = new NodeData(Arrays.asList(""), Arrays.asList(p));
            pc.add(n);
        }
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 200; j++) {
                Property p = new Property("common" + i, ValueType.STRING, "");
                NodeData n = new NodeData(Arrays.asList(""), Arrays.asList(p));
                pc.add(n);
            }
        }
        System.out.println(pc);
    }
}
