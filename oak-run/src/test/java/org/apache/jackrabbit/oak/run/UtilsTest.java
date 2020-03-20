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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;

import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilderHelper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

public class UtilsTest {

    @Test
    public void validateMongoUri() throws Exception {
        assumeTrue(MongoUtils.isAvailable());
        ImmutableList<String> args = ImmutableList.<String>builder().add(MongoUtils.URL)
                .build().asList();
        try (Closer closer = Closer.create()) {
            DocumentNodeStoreBuilder<?> builder = Utils.createDocumentMKBuilder(args.toArray(new String[args.size()]), closer, "");
            if(builder instanceof MongoDocumentNodeStoreBuilder) {
                MongoDocumentNodeStoreBuilder mb = (MongoDocumentNodeStoreBuilder)builder;
                String mongoUri = MongoDocumentNodeStoreBuilderHelper.getMongoUri(mb);
                assertNotNull(mongoUri);
                assertEquals(MongoUtils.URL, mongoUri);
            }
        }
    }
}
