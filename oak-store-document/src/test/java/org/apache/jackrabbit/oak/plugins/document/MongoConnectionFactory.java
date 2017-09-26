/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import java.util.List;

import javax.annotation.CheckForNull;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.rules.ExternalResource;

public class MongoConnectionFactory extends ExternalResource {

    private final List<MongoConnection> connections = Lists.newArrayList();

    @CheckForNull
    public MongoConnection getConnection() {
        MongoConnection c = MongoUtils.getConnection();
        if (c != null) {
            connections.add(c);
        }
        return c;
    }

    @CheckForNull
    public MongoConnection getConnection(String dbName) {
        MongoConnection c = MongoUtils.getConnection(dbName);
        if (c != null) {
            connections.add(c);
        }
        return c;
    }

    @Override
    protected void after() {
        for (MongoConnection c : connections) {
            c.close();
        }
    }
}
