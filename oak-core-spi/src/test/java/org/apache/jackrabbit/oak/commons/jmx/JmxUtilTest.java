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

package org.apache.jackrabbit.oak.commons.jmx;

import junit.framework.TestCase;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.Test;

import javax.management.ObjectName;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class JmxUtilTest {

    @Test
    public void quotation() {
        assertEquals("text", JmxUtil.quoteValueIfRequired("text"));
        TestCase.assertEquals("", JmxUtil.quoteValueIfRequired(""));
        assertTrue(JmxUtil.quoteValueIfRequired("text*with?chars").startsWith("\""));
    }

    @Test
    public void quoteAndComma() {
        assertTrue(JmxUtil.quoteValueIfRequired("text,withComma").startsWith("\""));
        assertTrue(JmxUtil.quoteValueIfRequired("text=withEqual").startsWith("\""));
    }
    
    @Test
    public void testCreateObjectNameMap() throws Exception {
        Map<String, ObjectName> m = JmxUtil.createObjectNameMap("type", "name", Collections.singletonMap("key", "value"));
        assertEquals(1, m.size());
        ObjectName objectName = m.get("jmx.objectname");
        assertNotNull(objectName);
        assertEquals(WhiteboardUtils.JMX_OAK_DOMAIN, objectName.getDomain());
        assertEquals(3, objectName.getKeyPropertyList().size());
    }

}