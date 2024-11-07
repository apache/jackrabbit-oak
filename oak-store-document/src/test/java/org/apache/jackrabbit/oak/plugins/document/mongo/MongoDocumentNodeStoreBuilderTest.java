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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import org.junit.Test;

import static java.util.Set.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MongoDocumentNodeStoreBuilderTest {

    @Test
    public void socketKeepAlive() {
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
        assertTrue(builder.isSocketKeepAlive());
    }

    @Test
    public void clientSessionDisabled() {
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
        assertFalse(builder.isClientSessionDisabled());
    }

    @Test
    public void throttlingDisabled() {
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
        assertFalse(builder.isThrottlingEnabled());
    }

    @Test
    public void throttlingFeatureToggleDisabled() {
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
        assertNull(builder.getDocStoreThrottlingFeature());
    }

    @Test
    public void fullGCDisabled() {
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
        assertFalse(builder.isFullGCEnabled());
    }

    @Test
    public void fullGCFeatureToggleDisabled() {
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
        assertNull(builder.getDocStoreFullGCFeature());
    }

    @Test
    public void fullGCIncludePathsNotEmpty() {
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
        builder.setFullGCIncludePaths(new String[] {"/foo"});
        assertFalse(builder.getFullGCIncludePaths().isEmpty());
        assertEquals(of("/foo"), builder.getFullGCIncludePaths());
    }

    @Test
    public void fullGCIncludePathsWithWrongPath() {
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
        builder.setFullGCIncludePaths(new String[] {"/foo", "wrongPath/"});
        assertFalse(builder.getFullGCIncludePaths().isEmpty());
        assertEquals(of("/foo"), builder.getFullGCIncludePaths());
    }

    @Test
    // OAK-11218
    public void fullGCExcludePathsEmptyString() {
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
        builder.setFullGCExcludePaths(new String[] {""});
        assertTrue(builder.getFullGCExcludePaths().isEmpty());
    }

    @Test
    public void fullGCExcludePathsNotEmpty() {
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
        builder.setFullGCExcludePaths(new String[] {"/foo"});
        assertFalse(builder.getFullGCExcludePaths().isEmpty());
        assertEquals(of("/foo"), builder.getFullGCExcludePaths());
    }

    @Test
    public void fullGCExcludePathsWithWrongPath() {
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
        builder.setFullGCExcludePaths(new String[] {"/foo", "//"});
        assertFalse(builder.getFullGCExcludePaths().isEmpty());
        assertEquals(of("/foo"), builder.getFullGCExcludePaths());
    }

    @Test
    public void embeddedVerificationEnabled() {
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
        assertTrue(builder.isEmbeddedVerificationEnabled());
    }

    @Test
    public void embeddedVerificationFeatureToggleEnabled() {
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
        assertNull(builder.getDocStoreEmbeddedVerificationFeature());
    }

    @Test
    public void collectionCompressionDisabled() {
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
        assertNull(builder.getCollectionCompressionType());
    }

    @Test
    public void fullGCModeDefaultValue() {
        MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
        final int fullGcModeNone = 0;
        assertEquals(builder.getFullGCMode(), fullGcModeNone);
    }
}
