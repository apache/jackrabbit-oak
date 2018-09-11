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
package org.apache.jackrabbit.oak.commons;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.*;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for class {@link LongUtils}.
 *
 * @date 27.06.2017
 * @see LongUtils
 **/
public class LongUtilsTest {


    @Test
    public void testSafeAddWithPositive() {

        assertEquals(9223372036854775807L, LongUtils.safeAdd(9223372036854775807L, 1392409284502L));

    }


    @Test
    public void testSafeAddWithNegative() {

        assertEquals(9223372036854775806L, LongUtils.safeAdd((-1L), 9223372036854775807L));

    }


}