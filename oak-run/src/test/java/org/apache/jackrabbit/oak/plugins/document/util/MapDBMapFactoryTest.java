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
package org.apache.jackrabbit.oak.plugins.document.util;

import java.util.concurrent.ConcurrentMap;

import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MapDBMapFactoryTest {

    @Test
    public void mapDB() {
        ConcurrentMap<Path, Revision> map = new MapDBMapFactory().create();
        for (int i = 0; i < 10000; i++) {
            map.put(Path.fromString("/some/test/path/node-" + i), new Revision(i, 0, 1));
        }
        for (int i = 0; i < 10000; i++) {
            assertEquals(
                    new Revision(i, 0, 1),
                    map.get(Path.fromString("/some/test/path/node-" + i))
            );
        }
    }
}
