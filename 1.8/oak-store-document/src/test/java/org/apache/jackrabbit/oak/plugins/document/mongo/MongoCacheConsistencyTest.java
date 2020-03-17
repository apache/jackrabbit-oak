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

import java.util.List;

import com.github.fakemongo.Fongo;
import com.mongodb.BulkWriteResult;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.OakFongo;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

import org.apache.jackrabbit.oak.plugins.document.CacheConsistencyTestBase;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture;
import org.junit.Rule;

public class MongoCacheConsistencyTest extends CacheConsistencyTestBase {

    @Rule
    public DocumentMKBuilderProvider provider = new DocumentMKBuilderProvider();

    private String exceptionMsg;

    @Override
    public DocumentStoreFixture getFixture() throws Exception {
        Fongo fongo = new OakFongo("fongo") {

            private String suppressedEx = null;

            @Override
            protected void afterInsert(WriteResult result) {
                maybeThrow();
            }

            @Override
            protected void afterFindAndModify(DBObject result) {
                maybeThrow();
            }

            @Override
            protected void afterUpdate(WriteResult result) {
                maybeThrow();
            }

            @Override
            protected void afterRemove(WriteResult result) {
                maybeThrow();
            }

            @Override
            protected void beforeExecuteBulkWriteOperation(boolean ordered,
                                                           Boolean bypassDocumentValidation,
                                                           List<?> writeRequests,
                                                           WriteConcern aWriteConcern) {
                // suppress potentially set exception message because
                // fongo bulk writes call other update methods
                suppressedEx = exceptionMsg;
                exceptionMsg = null;
            }

            @Override
            protected void afterExecuteBulkWriteOperation(BulkWriteResult result) {
                exceptionMsg = suppressedEx;
                suppressedEx = null;
                maybeThrow();
            }

            private void maybeThrow() {
                if (exceptionMsg != null) {
                    throw new MongoException(exceptionMsg);
                }
            }
        };
        DocumentMK.Builder builder = provider.newBuilder().setAsyncDelay(0);
        final DocumentStore store = new MongoDocumentStore(fongo.getDB("oak"), builder);
        return new DocumentStoreFixture() {
            @Override
            public String getName() {
                return "MongoDB";
            }

            @Override
            public DocumentStore createDocumentStore(DocumentMK.Builder builder) {
                return store;
            }
        };
    }

    @Override
    public void setTemporaryUpdateException(String msg) {
        this.exceptionMsg = msg;
    }
}
