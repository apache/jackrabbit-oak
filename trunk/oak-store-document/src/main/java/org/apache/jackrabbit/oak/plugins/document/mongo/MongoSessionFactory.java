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

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClient;
import com.mongodb.session.ClientSession;
import com.mongodb.session.ServerSession;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * Factory for {@link ClientSession}s.
 */
class MongoSessionFactory {

    private final MongoClient client;

    private final ClientSessionOptions options;

    private BsonDocument clusterTime;

    private BsonTimestamp operationTime;

    MongoSessionFactory(MongoClient client) {
        this.client = client;
        this.options = ClientSessionOptions.builder()
                .causallyConsistent(true).build();
    }

    ClientSession createClientSession() {
        ClientSession s = client.startSession(options);
        synchronized (this) {
            s.advanceClusterTime(clusterTime);
            s.advanceOperationTime(operationTime);
        }
        return new TrackingClientSession(s);
    }

    private class TrackingClientSession implements ClientSession {

        private final ClientSession session;

        TrackingClientSession(ClientSession session) {
            this.session = session;
        }

        @Override
        public ClientSessionOptions getOptions() {
            return session.getOptions();
        }

        @Override
        public boolean isCausallyConsistent() {
            return session.isCausallyConsistent();
        }

        @Override
        public Object getOriginator() {
            return session.getOriginator();
        }

        @Override
        public ServerSession getServerSession() {
            return session.getServerSession();
        }

        @Override
        public BsonTimestamp getOperationTime() {
            return session.getOperationTime();
        }

        @Override
        public void advanceOperationTime(BsonTimestamp operationTime) {
            session.advanceOperationTime(operationTime);
        }

        @Override
        public void advanceClusterTime(BsonDocument clusterTime) {
            session.advanceClusterTime(clusterTime);
        }

        @Override
        public BsonDocument getClusterTime() {
            return session.getClusterTime();
        }

        @Override
        public void close() {
            synchronized (this) {
                session.advanceClusterTime(clusterTime);
                clusterTime = session.getClusterTime();
                session.advanceOperationTime(operationTime);
                operationTime = session.getOperationTime();
            }
            session.close();
        }
    }
}
