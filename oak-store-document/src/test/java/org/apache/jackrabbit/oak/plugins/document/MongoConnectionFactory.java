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

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDockerRule;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.jetbrains.annotations.Nullable;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static org.junit.Assume.assumeNotNull;

public class MongoConnectionFactory extends ExternalResource {

    private final MongoDockerRule mongo = new MongoDockerRule();

    private final List<MongoConnection> connections = new ArrayList<>();

    @Override
    public Statement apply(Statement base, Description description) {
        Statement s = super.apply(base, description);
        if (MongoDockerRule.isDockerAvailable()) {
            s = mongo.apply(s, description);
        }
        return s;
    }

    @Nullable
    public MongoConnection getConnection() {
        return getConnection(MongoUtils.DB);
    }

    @Nullable
    public MongoConnection getConnection(String dbName) {
        if (DocumentStoreFixture.MongoFixture.SKIP_MONGO) {
            return null;
        }
        // first try MongoDB running on configured host and port
        MongoConnection c = MongoUtils.getConnection(dbName);
        if (c == null && MongoDockerRule.isDockerAvailable()) {
            // fall back to docker if available
            c = new MongoConnection(mongo.getHost(), mongo.getPort(), dbName);
        }
        assumeNotNull(c);
        if (c != null) {
            connections.add(c);
        }
        return c;
    }

    @Override
    protected void after() {
        for (MongoConnection c : connections) {
            try {
                c.close();
            } catch (IllegalStateException e) {
                // may happen when connection is already closed (OAK-7447)
            }
        }
    }
}
