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
package org.apache.jackrabbit.oak.plugins.document.rdb;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.junit.Test;

public class RDBDocumentNodeStoreBuilderTest {

    @Test
    public void testReadOnlyDS() throws Exception {
        // see OAK-8214

        DataSource dataSource = RDBDataSourceFactory.forJdbcUrl("jdbc:h2:mem:" + UUID.randomUUID(), "", "");
        RDBDocumentNodeStoreBuilder b = RDBDocumentNodeStoreBuilder.newRDBDocumentNodeStoreBuilder().setRDBConnection(dataSource).setReadOnlyMode();

        try {
            b.getDocumentStore();
            fail("should not get here");
        } catch (DocumentStoreException expected) {
        }
    }

    @Test
    public void testReadOnlyBS() throws Exception {
        // see OAK-8251

        DataSource dataSource = RDBDataSourceFactory.forJdbcUrl("jdbc:h2:mem:" + UUID.randomUUID(), "", "");
        RDBDocumentNodeStoreBuilder b = RDBDocumentNodeStoreBuilder.newRDBDocumentNodeStoreBuilder().setRDBConnection(dataSource).setReadOnlyMode();

        try {
            b.getBlobStore();
            fail("should not get here");
        } catch (DocumentStoreException expected) {
        }
    }

    @Test
    public void fullGCDisabled() {
        RDBDocumentNodeStoreBuilder builder = new RDBDocumentNodeStoreBuilder();
        builder.setFullGCEnabled(true);
        assertFalse(builder.isFullGCEnabled());
    }

    @Test
    public void fullFGCFeatureToggleDisabled() {
        RDBDocumentNodeStoreBuilder builder = new RDBDocumentNodeStoreBuilder();
        Feature docStoreFullGCFeature = mock(Feature.class);
        when(docStoreFullGCFeature.isEnabled()).thenReturn(true);
        builder.setDocStoreFullGCFeature(docStoreFullGCFeature);
        assertNull(builder.getDocStoreFullGCFeature());
    }

    @Test
    public void embeddedVerificationDisabled() {
        RDBDocumentNodeStoreBuilder builder = new RDBDocumentNodeStoreBuilder();
        builder.setEmbeddedVerificationEnabled(true);
        assertFalse(builder.isEmbeddedVerificationEnabled());
    }

    @Test
    public void embeddedVerificationFeatureToggleDisabled() {
        RDBDocumentNodeStoreBuilder builder = new RDBDocumentNodeStoreBuilder();
        Feature embeddedVerificationFeature = mock(Feature.class);
        when(embeddedVerificationFeature.isEnabled()).thenReturn(true);
        builder.setDocStoreEmbeddedVerificationFeature(embeddedVerificationFeature);
        assertNull(builder.getDocStoreEmbeddedVerificationFeature());
    }
}
