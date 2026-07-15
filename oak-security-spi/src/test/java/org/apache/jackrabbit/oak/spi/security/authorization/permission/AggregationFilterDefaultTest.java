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
package org.apache.jackrabbit.oak.spi.security.authorization.permission;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.junit.Test;

import javax.jcr.security.AccessControlManager;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

public class AggregationFilterDefaultTest {

    @Test
    public void testStopPermissionProvider() {
        assertFalse(AggregationFilter.DEFAULT.stop(mock(AggregatedPermissionProvider.class), Collections.EMPTY_SET));
    }

    @Test
    public void testStopEffectivePoliciesByPath() {
        assertFalse(AggregationFilter.DEFAULT.stop(mock(AccessControlManager.class), null));
        assertFalse(AggregationFilter.DEFAULT.stop(mock(AccessControlManager.class), PathUtils.ROOT_PATH));
    }

    @Test
    public void testStopEffectivePoliciesByPrincipals() {
        assertFalse(AggregationFilter.DEFAULT.stop(mock(JackrabbitAccessControlManager.class), Collections.EMPTY_SET));
    }
}