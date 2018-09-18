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

import java.util.concurrent.atomic.AtomicReference;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

import org.jetbrains.annotations.NotNull;

class MongoTestClient extends MongoClient {

    private AtomicReference<String> beforeQueryException = new AtomicReference<>();
    private AtomicReference<String> beforeUpdateException = new AtomicReference<>();
    private AtomicReference<String> afterUpdateException = new AtomicReference<>();

    MongoTestClient(String uri) {
        super(new MongoClientURI(uri));
    }

    @NotNull
    @Override
    public MongoDatabase getDatabase(String databaseName) {
        return new MongoTestDatabase(super.getDatabase(databaseName),
                beforeQueryException, beforeUpdateException, afterUpdateException);
    }

    void setExceptionBeforeQuery(String msg) {
        beforeQueryException.set(msg);
    }

    void setExceptionBeforeUpdate(String msg) {
        beforeUpdateException.set(msg);
    }

    void setExceptionAfterUpdate(String msg) {
        afterUpdateException.set(msg);
    }
}
