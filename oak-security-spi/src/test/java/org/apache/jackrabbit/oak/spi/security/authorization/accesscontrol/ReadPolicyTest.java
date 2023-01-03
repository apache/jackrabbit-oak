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
package org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReadPolicyTest {

    @Test
    public void testGetName() throws Exception {
        assertEquals("Grants read access on configured trees.", ReadPolicy.INSTANCE.getName());
    }
    
    @Test
    public void testHasEffectiveReadPolicyNullPath() {
        assertFalse(ReadPolicy.hasEffectiveReadPolicy(Collections.emptySet(), null));
        assertFalse(ReadPolicy.hasEffectiveReadPolicy(Collections.singleton(PathUtils.ROOT_PATH), null));
        assertFalse(ReadPolicy.hasEffectiveReadPolicy(ImmutableSet.of("/some/path", "/another/path"), null));
    }

    @Test
    public void testHasEffectiveReadPolicy() {
        String path = "/some/random/path";
        assertFalse(ReadPolicy.hasEffectiveReadPolicy(Collections.emptySet(), path));
        assertFalse(ReadPolicy.hasEffectiveReadPolicy(Collections.singleton("/another/path"), path));
        assertFalse(ReadPolicy.hasEffectiveReadPolicy(Collections.singleton(path+"-sibling"), path));
        assertFalse(ReadPolicy.hasEffectiveReadPolicy(Collections.singleton(path+"/child"), path));
        
        assertTrue(ReadPolicy.hasEffectiveReadPolicy(Collections.singleton(path), path));
        assertTrue(ReadPolicy.hasEffectiveReadPolicy(Collections.singleton(PathUtils.ROOT_PATH), path));
        assertTrue(ReadPolicy.hasEffectiveReadPolicy(ImmutableSet.of("/some/random"), path));
        assertTrue(ReadPolicy.hasEffectiveReadPolicy(ImmutableSet.of("/another/path", "/some/random/path"), path));
        assertTrue(ReadPolicy.hasEffectiveReadPolicy(ImmutableSet.of("/another/path", PathUtils.ROOT_PATH), path));
    }
}