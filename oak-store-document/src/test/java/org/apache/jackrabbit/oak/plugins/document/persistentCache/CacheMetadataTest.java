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
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link CacheMetadata}.
 */
public class CacheMetadataTest {

    private static final String KEY = "key";

    private CacheMetadata<String, StringValue> metadata;

    private StringValue value;

    private StringValue equalValue;

    @Before
    public void setUp() {
        metadata = new CacheMetadata<>();
        value = new StringValue("v");
        equalValue = new StringValue("v");
    }

    @Test
    public void incrementCreatesEntryForValue() {
        metadata.increment(KEY, value);
        metadata.increment(KEY, value);

        CacheMetadata.MetadataEntry entry = metadata.remove(KEY, value);
        Assert.assertNotNull(entry);
        Assert.assertEquals(2, entry.getAccessCount());
        Assert.assertFalse(entry.isReadFromPersistentCache());
    }

    @Test
    public void putFromPersistenceMarksEntry() {
        metadata.putFromPersistenceAndIncrement(KEY, value);

        CacheMetadata.MetadataEntry entry = metadata.remove(KEY, value);
        Assert.assertNotNull(entry);
        Assert.assertEquals(1, entry.getAccessCount());
        Assert.assertTrue(entry.isReadFromPersistentCache());
    }

    @Test
    public void incrementIfPresentIgnoresMissingEntry() {
        metadata.incrementIfPresent(KEY, value);

        Assert.assertNull(metadata.remove(KEY, value));
    }

    @Test
    public void incrementIfPresentIgnoresOtherValue() {
        metadata.put(KEY, value);
        metadata.incrementIfPresent(KEY, equalValue);
        metadata.incrementIfPresent(KEY, value);

        Assert.assertEquals(1, metadata.remove(KEY, value).getAccessCount());
    }

    @Test
    public void removeWithOtherValueKeepsEntry() {
        metadata.increment(KEY, value);

        Assert.assertNull(metadata.remove(KEY, equalValue));
        Assert.assertNotNull(metadata.remove(KEY, value));
        Assert.assertNull(metadata.remove(KEY, value));
    }

    @Test
    public void putWithOtherValueReplacesEntry() {
        metadata.putFromPersistenceAndIncrement(KEY, value);
        metadata.put(KEY, equalValue);

        Assert.assertNull(metadata.remove(KEY, value));
        CacheMetadata.MetadataEntry entry = metadata.remove(KEY, equalValue);
        Assert.assertNotNull(entry);
        Assert.assertEquals(0, entry.getAccessCount());
        Assert.assertFalse(entry.isReadFromPersistentCache());
    }

    @Test
    public void putWithSameValueKeepsEntry() {
        metadata.increment(KEY, value);
        metadata.put(KEY, value);

        Assert.assertEquals(1, metadata.remove(KEY, value).getAccessCount());
    }

    @Test
    public void removeByKeyAndBulkOperations() {
        metadata.put(KEY, value);
        metadata.put("other", value);
        Assert.assertNotNull(metadata.remove(KEY));

        metadata.put(KEY, value);
        metadata.removeAll(List.of(KEY));
        Assert.assertNull(metadata.remove(KEY, value));

        metadata.clear();
        Assert.assertNull(metadata.remove("other", value));
    }

    @Test
    public void disabledMetadataIgnoresAllOperations() {
        metadata.disable();
        Assert.assertFalse(metadata.isEnabled());

        metadata.put(KEY, value);
        metadata.increment(KEY, value);
        metadata.putFromPersistenceAndIncrement(KEY, value);
        metadata.incrementIfPresent(KEY, value);
        metadata.removeAll(List.of(KEY));
        metadata.clear();

        Assert.assertNull(metadata.remove(KEY));
        Assert.assertNull(metadata.remove(KEY, value));
    }
}
