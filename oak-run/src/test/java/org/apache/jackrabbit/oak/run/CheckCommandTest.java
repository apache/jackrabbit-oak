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
package org.apache.jackrabbit.oak.run;

import org.apache.jackrabbit.oak.segment.azure.tool.AzureCheck;
import org.apache.jackrabbit.oak.segment.azure.tool.AzureCheck.PersistentCacheType;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CheckCommandTest {
    @Test
    public void selectsLegacyByDefaultAndAllowsBothV2Modes() throws Exception {
        assertCacheType(PersistentCacheType.LEGACY, "az:store", "--persistent-cache-path", "cache");
        assertCacheType(PersistentCacheType.SYNC, "az:store", "--persistent-cache-path", "cache",
                "--persistent-cache-type", "sync");
        assertCacheType(PersistentCacheType.ASYNC, "az:store", "--persistent-cache-path", "cache",
                "--persistent-cache-type", "async");
    }

    @Test
    public void acceptsCacheTypesIgnoringCase() throws Exception {
        assertCacheType(PersistentCacheType.LEGACY, "az:store", "--persistent-cache-path", "cache",
                "--persistent-cache-type", "Legacy");
        assertCacheType(PersistentCacheType.SYNC, "az:store", "--persistent-cache-path", "cache",
                "--persistent-cache-type", "SYNC");
        assertCacheType(PersistentCacheType.ASYNC, "az:store", "--persistent-cache-path", "cache",
                "--persistent-cache-type", "aSyNc");
    }

    @Test
    public void rejectsInvalidCacheTypeUsage() throws Exception {
        CheckCommand command = new CheckCommand();
        assertEquals(1, command.run("store", "--persistent-cache-path", "cache", "--persistent-cache-type", "sync"));
        assertEquals(1, command.run("az:store", "--persistent-cache-type", "sync"));
        assertEquals(1, command.run("az:store", "--persistent-cache-path", "cache", "--persistent-cache-type", "unknown"));
    }

    private static void assertCacheType(PersistentCacheType expected, String... args) throws Exception {
        AzureCheck.Builder builder = mock(AzureCheck.Builder.class, RETURNS_SELF);
        AzureCheck check = mock(AzureCheck.class);
        when(builder.build()).thenReturn(check);
        try (MockedStatic<AzureCheck> azureCheck = mockStatic(AzureCheck.class)) {
            azureCheck.when(AzureCheck::builder).thenReturn(builder);
            assertEquals(0, new CheckCommand().run(args));
            verify(builder).withPersistentCacheType(expected);
            verify(builder).withPersistentCachePath("cache");
            verify(builder).withPersistentCacheSizeGb(50);
            verify(check).run();
        }
    }
}
