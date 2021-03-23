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

import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcernException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.connection.ServerVersion;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.internal.connection.MongoWriteConcernWithResponseException;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException.Type;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Provides static utility methods for MongoDB.
 */
public class MongoUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MongoUtils.class);

    /**
     * Forces creation of an index on a field, if one does not already exist.
     *
     * @param collection the collection.
     * @param field the name of the field.
     * @param ascending {@code true} for an ascending, {@code false} for a
     *                              descending index.
     * @param unique whether values are unique.
     * @param sparse whether the index should be sparse.
     * @throws MongoException if the operation fails.
     */
    static void createIndex(MongoCollection<?> collection,
                            String field,
                            boolean ascending,
                            boolean unique,
                            boolean sparse)
            throws MongoException {
        createIndex(collection, new String[]{field},
                new boolean[]{ascending}, unique, sparse);
    }

    /**
     * Forces creation of an index on a set of fields, if one does not already
     * exist.
     *
     * @param collection the collection.
     * @param fields the name of the fields.
     * @param ascending {@code true} for an ascending, {@code false} for a
     *                              descending index.
     * @param unique whether values are unique.
     * @param sparse whether the index should be sparse.
     * @throws IllegalArgumentException if {@code fields} and {@code ascending}
     *          arrays have different lengths.
     * @throws MongoException if the operation fails.
     */
    static void createIndex(MongoCollection<?> collection,
                            String[] fields,
                            boolean[] ascending,
                            boolean unique,
                            boolean sparse)
            throws MongoException {
        checkArgument(fields.length == ascending.length);
        BasicDBObject index = new BasicDBObject();
        for (int i = 0; i < fields.length; i++) {
            index.put(fields[i], ascending[i] ? 1 : -1);
        }
        IndexOptions options = new IndexOptions().unique(unique).sparse(sparse);
        collection.createIndex(index, options);
    }

    /**
     * Forces creation of a partial index on a set of fields, if one does not
     * already exist.
     *
     * @param collection the collection.
     * @param fields the name of the fields.
     * @param ascending {@code true} for an ascending, {@code false} for a
     *                              descending index.
     * @param filter the filter expression for the partial index.
     * @throws MongoException if the operation fails.
     */
    static void createPartialIndex(MongoCollection<?> collection,
                                   String[] fields,
                                   boolean[] ascending,
                                   String filter) throws MongoException {
        checkArgument(fields.length == ascending.length);
        BasicDBObject index = new BasicDBObject();
        for (int i = 0; i < fields.length; i++) {
            index.put(fields[i], ascending[i] ? 1 : -1);
        }
        IndexOptions options = new IndexOptions().partialFilterExpression(BasicDBObject.parse(filter));
        collection.createIndex(index, options);
    }

    /**
     * Returns {@code true} if there is an index on the given fields,
     * {@code false} otherwise. If multiple fields are passed, this method
     * check if there a compound index on those field. This method does not
     * check the sequence of fields for a compound index. That is, this method
     * will return {@code true} as soon as it finds an index that covers the
     * given fields, no matter their sequence in the compound index.
     *
     * @param collection the collection.
     * @param fields the fields of an index.
     * @return {@code true} if the index exists, {@code false} otherwise.
     * @throws MongoException if the operation fails.
     */
    static boolean hasIndex(MongoCollection<?> collection, String... fields)
            throws MongoException {
        Set<String> uniqueFields = Sets.newHashSet(fields);
        for (Document info : collection.listIndexes()) {
            Document key = (Document) info.get("key");
            Set<String> indexFields = Sets.newHashSet(key.keySet());
            if (uniqueFields.equals(indexFields)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns {@code true} if the given collection is empty, {@code false}
     * otherwise. The check always happens on the MongoDB primary.
     *
     * @param collection the collection to check.
     * @param session an optional client session.
     * @return {@code true} if empty, {@code false} otherwise.
     */
    static boolean isCollectionEmpty(@NotNull MongoCollection<?> collection,
                                     @Nullable ClientSession session) {
        MongoCollection<?> c = collection.withReadPreference(ReadPreference.primary());
        FindIterable<BasicDBObject> result;
        if (session != null) {
            result = c.find(session, BasicDBObject.class);
        } else {
            result = c.find(BasicDBObject.class);
        }
        return result.limit(1).first() == null;
    }

    /**
     * Returns the {@code DocumentStoreException} {@link Type} for the given
     * throwable.
     *
     * @param t the throwable.
     * @return the type.
     */
    static Type getDocumentStoreExceptionTypeFor(Throwable t) {
        Type type = Type.GENERIC;
        if (t instanceof MongoSocketException
                || t instanceof MongoWriteConcernException
                || t instanceof MongoNotPrimaryException) {
            type = Type.TRANSIENT;
        } else if (t instanceof MongoCommandException
                || t instanceof WriteConcernException
                || t instanceof MongoWriteConcernWithResponseException) {
            int code = ((MongoException) t).getCode();
            if (code == 11600               // InterruptedAtShutdown
                    || code == 11601        // Interrupted
                    || code == 11602) {     // InterruptedDueToReplStateChange
                type = Type.TRANSIENT;
            }
        }
        return type;
    }

    /**
     * Util method to get node size limit for MongoDB server version from the
     * MongoDB status.
     *
     * @param status the MongoDB status
     * @return size limit based on MongoDB version.
     */
    static int getNodeNameLimit(final @NotNull MongoStatus status) {
        return status.isVersion(4, 2) ? Integer.MAX_VALUE : Utils.NODE_NAME_LIMIT;
    }

    /**
     * Returns {@code true} if the MongoDB server is part of a Replica Set,
     * {@code false} otherwise. The Replica Set Status is achieved by the
     * ReplicaSetStatusListener, which is triggered whenever the cluster
     * description changes.
     *
     * @param client the mongo client.
     * @return {@code true} if part of Replica Set, {@code false} otherwise.
     */
    public static boolean isReplicaSet(@NotNull MongoClient client) {
        OakClusterListener listener = getOakClusterListener(client);
        if (listener != null) {
            return listener.isReplicaSet();
        }
        System.out.println("Method isReplicaSet called for a MongoClient without any OakClusterListener!");
        LOG.warn("Method isReplicaSet called for a MongoClient without any OakClusterListener!");
        return false;
    }

    /**
     * Returns the {@ServerAddress} of the MongoDB cluster.
     *
     * @param client the mongo client.
     * @return {@ServerAddress} of the cluster. {@null} if not connected or no listener was available.
     */
    public static ServerAddress getAddress(@NotNull MongoClient client) {
        OakClusterListener listener = getOakClusterListener(client);
        if (listener != null) {
            return listener.getServerAddress();
        }
        System.out.println("Method getAddress called for a MongoClient without any OakClusterListener!");
        LOG.warn("Method getAddress called for a MongoClient without any OakClusterListener!");
        return null;
    }

    /**
     * Returns the {@ServerAddress} of the MongoDB cluster primary node.
     *
     * @param client the mongo client.
     * @return {@ServerAddress} of the primary. {@null} if not connected or no listener was available.
     */
    public static ServerAddress getPrimaryAddress(@NotNull MongoClient client) {
        OakClusterListener listener = getOakClusterListener(client);
        if (listener != null) {
            return listener.getPrimaryAddress();
        }
        return null;
    }

    private static OakClusterListener getOakClusterListener(@NotNull MongoClient client) {
        for (ClusterListener clusterListener : client.getMongoClientOptions().getClusterListeners()) {
            if (clusterListener instanceof OakClusterListener) {
                OakClusterListener replClusterListener = (OakClusterListener) clusterListener;
                return replClusterListener;
            }
        }
        System.out.println("No OakClusterListener found for this MongoClient!");
        LOG.warn("No OakClusterListener found for this MongoClient!");
        return null;
    }

    public static class OakClusterListener implements ClusterListener {

        private boolean replicaSet = false;
        private boolean connected = false;
        private ServerAddress serverAddress;
        private ServerAddress primaryAddress;

        public ServerAddress getServerAddress() {
            return serverAddress;
        }

        public ServerAddress getPrimaryAddress() {
            return primaryAddress;
        }

        public boolean isReplicaSet() {
            // Sometimes we need to wait a few seconds in case the connection was just created, the listener
            // didn't have time to receive the description from the cluster.
            try {
                int trials = 30;
                while (!connected && trials > 0) {
                    trials--;
                    Thread.sleep(500);
                }
            } catch (InterruptedException e) {}
            return replicaSet;
        }

        @Override
        public void clusterOpening(ClusterOpeningEvent event) {
        }

        @Override
        public void clusterClosed(ClusterClosedEvent event) {
        }

        @Override
        public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
            for (ServerDescription sd : event.getNewDescription().getServerDescriptions()) {
                if (sd.getState() == ServerConnectionState.CONNECTED) {
                    connected = true;
                    serverAddress = sd.getAddress();
                    primaryAddress = new ServerAddress(sd.getPrimary());
                    if (sd.isReplicaSetMember()) {
                        // Can't assign directly the result of the function because in some cases the cluster
                        // type is UNKNOWN, mainly when the cluster is changing it's PRIMARY.
                        replicaSet = true;
                    }
                }
            }
        }
    }
}
