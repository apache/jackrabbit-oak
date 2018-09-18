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
package org.apache.jackrabbit.oak.composite;

import org.junit.Assert;
import org.junit.Test;

public class StringCacheTest {

    @Test
    public void testCache() {
        StringCache cache = new StringCache();

        StringBuilder b = new StringBuilder("abc").append("xyz");

        String s1 = b.toString();
        String s2 = b.toString();
        String s3 = b.toString();

        Assert.assertNotSame(s1, s2);
        Assert.assertNotSame(s2, s3);
        Assert.assertNotSame(s1, s3);

        String c1 = cache.get(s1);
        String c2 = cache.get(s2);
        String c3 = cache.get(s3);

        Assert.assertSame(c1, c2);
        Assert.assertSame(c2, c3);
        Assert.assertSame(c1, c3);
    }

}
