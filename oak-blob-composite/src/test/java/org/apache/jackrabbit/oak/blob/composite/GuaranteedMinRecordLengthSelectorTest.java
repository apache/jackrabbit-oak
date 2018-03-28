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

package org.apache.jackrabbit.oak.blob.composite;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class GuaranteedMinRecordLengthSelectorTest {
    private DelegateHandler delegateHandler;
    private DelegateMinRecordLengthSelector minRecordLengthSelector;

    private static final String MRL_KEY = "minRecordLength";

    @Before
    public void setup() {
        delegateHandler = new IntelligentDelegateHandler();
        minRecordLengthSelector = new GuaranteedMinRecordLengthSelector();
    }

    private DelegateDataStore createDelegateDataStore(final String role, int minRecordLength) {
        return createDelegateDataStore(role, minRecordLength, false);
    }

    private DelegateDataStore createDelegateDataStore(final String role, int minRecordLength, boolean readOnly) {
        Map<String, Object> config = Maps.newHashMap();
        config.put(DataStoreProvider.ROLE, role);
        config.put(MRL_KEY, minRecordLength);
        if (readOnly) {
            config.put("readOnly", true);
        }
        return createDelegateDataStore(role, config);
    }

    private DelegateDataStore createDelegateDataStore(final String role, final Map<String, Object> config) {
        return new DelegateDataStore(new DataStoreProvider() {
            private TestableFileDataStore ds = new TestableFileDataStore(config);
            @Override
            public DataStore getDataStore() {
                return ds;
            }

            @Override
            public String getRole() {
                return role;
            }
        }, config);
    }

    @Test
    public void testSingleDelegateReturnsDelegateMinRecLen() {
        String role = "role1";
        int len = 4096;
        delegateHandler.addDelegateDataStore(createDelegateDataStore(role, len));
        assertEquals(len, minRecordLengthSelector.getMinRecordLength(delegateHandler));
    }

    @Test
    public void testMultipleWritableDelegatesReturnsOverallMinRecLen() {
        String role1 = "role1";
        String role2 = "role2";
        int len1 = 4096;
        int len2 = 8192;
        delegateHandler.addDelegateDataStore(createDelegateDataStore(role1, len1));
        delegateHandler.addDelegateDataStore(createDelegateDataStore(role2, len2));
        assertEquals(len1, minRecordLengthSelector.getMinRecordLength(delegateHandler));
    }

    @Test
    public void testGetMinRecLenIgnoresReadonlyDelegates() {
        String role1 = "role1";
        String role2 = "role2";
        int len1 = 4096;
        int len2 = 8192;
        delegateHandler.addDelegateDataStore(createDelegateDataStore(role1, len1, true));
        delegateHandler.addDelegateDataStore(createDelegateDataStore(role2, len2));
        assertEquals(len2, minRecordLengthSelector.getMinRecordLength(delegateHandler));
    }

    static class TestableFileDataStore extends OakFileDataStore {
        private static int defaultMinRecordLength = 1024*16;
        private int minRecordLength = defaultMinRecordLength;
        TestableFileDataStore(final Map<String, Object> config) {
            Object o = config.get("minRecordLength");
            if (null != o) {
                minRecordLength = (int) o;
            }
            this.setMinRecordLength(minRecordLength);
        }
    }
}
