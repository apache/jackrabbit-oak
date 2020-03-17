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
package org.apache.jackrabbit.oak.upgrade.cli.blob;

import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class S3DataStoreFactoryTest {

    @Test
    public void testPopulateProperties() throws NoSuchFieldException, IllegalAccessException {
        Properties props = new Properties();
        props.setProperty("cacheSize", "123");

        S3DataStore ds = S3DataStoreFactory.createDS("xyz", props);
        assertEquals(123, readLong("cacheSize", AbstractSharedCachingDataStore.class, ds));
    }

    @Test
    public void testStripOsgiPrefix() throws NoSuchFieldException, IllegalAccessException {
        Properties props = new Properties();
        props.setProperty("cacheSize", "I\"123\"");

        S3DataStore ds = S3DataStoreFactory.createDS("xyz", props);
        assertEquals(123, readLong("cacheSize", AbstractSharedCachingDataStore.class, ds));
    }

    private static long readLong(String fieldName, Class<?> clazz, Object object) throws NoSuchFieldException, IllegalAccessException {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getLong(object);
    }
}
