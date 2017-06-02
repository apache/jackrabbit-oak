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
package org.apache.jackrabbit.oak.commons.hash;

import junit.framework.TestCase;

public class SipHashTest extends TestCase {
    public void testSipHash() {
        assertEquals(0x1505140C, new SipHash(0x0L).hashCode());
        assertEquals(0x1505140C, new SipHash(0x01L).hashCode());
        assertEquals(0x1505140C, new SipHash(0xFFFFL).hashCode());

        assertEquals(0xE9B496CD, new SipHash(new SipHash(0x0L), 0x0L).hashCode());
        assertEquals(0xe9B46932, new SipHash(new SipHash(0x0L), 0xFFFFL).hashCode());
        assertEquals(0xD5A6998E, new SipHash(new SipHash(0xFFFFL), 0xFFFFL).hashCode());
    }
}
