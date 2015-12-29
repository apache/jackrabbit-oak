/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.rdb;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class RDBToolsTest {

    @Test
    public void testAppendInCondition() {
        assertEquals("", appendInCondition("ID", 0, 1000));
        assertEquals("ID = ?", appendInCondition("ID", 1, 1000));
        assertEquals("ID in (?,?,?)", appendInCondition("ID", 3, 1000));
        assertEquals("(ID in (?,?,?) or ID in (?,?,?) or ID in (?,?,?))", appendInCondition("ID", 9, 3));
        assertEquals("(ID in (?,?,?) or ID in (?,?,?) or ID in (?,?,?) or ID in (?,?))", appendInCondition("ID", 11, 3));
    }

    private String appendInCondition(String field, int placeholdersCount, int maxListLength) {
        StringBuilder builder = new StringBuilder();
        RDBJDBCTools.appendInCondition(builder, field, placeholdersCount, maxListLength);
        return builder.toString();
    }
}
