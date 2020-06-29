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
package org.apache.jackrabbit.oak.run.cli;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;

import com.google.common.io.Closer;

import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.replica.NodeCollectionProvider;
import org.apache.jackrabbit.oak.plugins.document.mongo.replica.ReplicaSetInfo;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.Ignore;
import org.junit.Test;

import joptsimple.OptionParser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

@Ignore("OAK-9125")
public class DocumentFixtureProviderTest {

    @Test
    @SuppressWarnings("UnstableApiUsage")
    public void validateMongoUri() throws Exception {
        assumeTrue(MongoUtils.isAvailable());
        Options options = createMongoOptions();
        BlobStore bs = new MemoryBlobStore();
        Whiteboard wb = new DefaultWhiteboard();
        wb.register(StatisticsProvider.class, StatisticsProvider.NOOP, Collections.emptyMap());
        try (Closer closer = Closer.create()) {
            DocumentNodeStore ns = DocumentFixtureProvider.configureDocumentMk(options, bs, wb, closer, false);
            MongoDocumentStore ds = getField(ns, "nonLeaseCheckingStore", MongoDocumentStore.class);
            ReplicaSetInfo info = getField(ds, "replicaInfo", ReplicaSetInfo.class);
            NodeCollectionProvider ncp = getField(info, "nodeCollections", NodeCollectionProvider.class);
            String uri = getField(ncp, "originalMongoUri", String.class);
            assertEquals(MongoUtils.URL, uri);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Object object, String fieldName, Class<T> returnType)
            throws Exception {
        Field f = object.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        Object obj = f.get(object);
        if (obj == null || returnType.isAssignableFrom(obj.getClass())) {
            return (T) obj;
        } else {
            fail(fieldName + " is not a " + returnType.getName());
        }
        throw new IllegalStateException();
    }

    private Options createMongoOptions() throws IOException {
        OptionParser parser = new OptionParser();
        Options opts = new Options().withDisableSystemExit();
        opts.parseAndConfigure(parser, new String[] {MongoUtils.URL});
        return opts;
    }
}
