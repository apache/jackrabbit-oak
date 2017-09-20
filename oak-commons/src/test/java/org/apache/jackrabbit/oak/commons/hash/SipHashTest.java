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

import org.junit.Test;

import static org.junit.Assert.*;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for class {@link SipHash}.
 *
 * @date 27.06.2017
 * @see SipHash
 **/
public class SipHashTest {
    @Test
    public void testCreatesSipHashTakingThreeArguments() {
        SipHash sipHash = new SipHash(4553L);
        SipHash sipHashTwo = new SipHash(sipHash, (-813L));

        assertFalse(sipHashTwo.equals(sipHash));
    }
}
