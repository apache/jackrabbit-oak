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
import com.mongodb.client.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.TransactionBody;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.session.ServerSession;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.jetbrains.annotations.NotNull;

/**
 * Factory for {@link ClientSession}s.
 */
class MongoSessionFactory {

    private final MongoClient client;

    private final ClientSessionOptions options;

    private final MongoClock clock;

    private BsonDocument clusterTime;

    private BsonTimestamp operationTime;

    MongoSessionFactory(@NotNull MongoClient client,
                        @NotNull MongoClock clock) {
        this.client = client;
        this.clock = clock;
        this.options = ClientSessionOptions.builder()
                .causallyConsistent(true).build();
    }

    ClientSession createClientSession() {
        ClientSession s = client.startSession(options);
        clock.advanceSession(s);
        return new TrackingClientSession(s);
    }

    private class TrackingClientSession implements ClientSession {

        private final ClientSession session;

        TrackingClientSession(ClientSession session) {
            this.session = session;
        }

        @Override
        @NotNull
        public ClientSessionOptions getOptions() {
            return session.getOptions();
        }

        @Override
        public boolean isCausallyConsistent() {
            return session.isCausallyConsistent();
        }

        @Override
        @NotNull
        public Object getOriginator() {
            return session.getOriginator();
        }

        @Override
        @NotNull
        public ServerSession getServerSession() {
            return session.getServerSession();
        }

        @Override
        @NotNull
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
        @NotNull
        public BsonDocument getClusterTime() {
            return session.getClusterTime();
        }

        @Override
        public boolean hasActiveTransaction() {
            return session.hasActiveTransaction();
        }

        @Override
        public boolean notifyMessageSent() {
            return session.notifyMessageSent();
        }

        @NotNull
        @Override
        public TransactionOptions getTransactionOptions() {
            return session.getTransactionOptions();
        }

        @Override
        public void startTransaction() {
            session.startTransaction();
        }

        @Override
        public void startTransaction(@NotNull TransactionOptions options) {
            session.startTransaction(options);
        }

        @Override
        public void commitTransaction() {
            session.commitTransaction();
        }

        @Override
        public void abortTransaction() {
            session.abortTransaction();
        }

        @Override
        public ServerAddress getPinnedServerAddress() {
            return session.getPinnedServerAddress();
        }

        @NotNull
        @Override
        public <T> T withTransaction(@NotNull TransactionBody<T> transactionBody) {
            return session.withTransaction(transactionBody);
        }

        @NotNull
        @Override
        public <T> T withTransaction(@NotNull TransactionBody<T> transactionBody,
                                     @NotNull TransactionOptions options) {
            return session.withTransaction(transactionBody, options);
        }

        @Override
        public BsonDocument getRecoveryToken() {
            return session.getRecoveryToken();
        }

        @Override
        public void setRecoveryToken(@NotNull BsonDocument recoveryToken) {
            session.setRecoveryToken(recoveryToken);
        }

        @Override
        public void close() {
            clock.advanceSessionAndClock(session);
            session.close();
        }

        @Override
        public Object getTransactionContext() {
            return session.getTransactionContext();
        }

        @Override
        public void setTransactionContext(@NotNull ServerAddress address, @NotNull Object transactionContext) {
            session.setTransactionContext(address, transactionContext);

        }

        @Override
        public void clearTransactionContext() {
            session.clearTransactionContext();

        }

        @Override
        public void setSnapshotTimestamp(BsonTimestamp snapshotTimestamp) {
            session.setSnapshotTimestamp(snapshotTimestamp);

        }

        @Override
        public BsonTimestamp getSnapshotTimestamp() {
            return session.getSnapshotTimestamp();
        }

        @Override
        public TimeoutContext getTimeoutContext() {
            return session.getTimeoutContext();
        }

        @Override
        public void notifyOperationInitiated(@NotNull Object operation) {
            session.notifyOperationInitiated(operation);
        }
    }
}
