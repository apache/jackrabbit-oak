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
package org.apache.jackrabbit.oak.jcr;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;

/**
 * abstract class that can be inherited by an IT who has to run tests against a cluster of
 * DocumentMKs for having some utility methods available.
 */
public abstract class DocumentClusterIT {
    List<Repository> repos = new ArrayList<Repository>();
    List<DocumentMK> mks = new ArrayList<DocumentMK>();

    /**
     * the number of nodes we'd like to run against
     */
    static final int NUM_CLUSTER_NODES = Integer.getInteger("it.documentmk.cluster.nodes", 5);
    
    /**
     * credentials for logging in as {@code admin}
     */
    static final Credentials ADMIN = new SimpleCredentials("admin", "admin".toCharArray());
    
    static final int NOT_PROVIDED = Integer.MIN_VALUE;
    
    @Before
    public void before() throws Exception {
        dropDB(this.getClass());
        
        List<Repository> rs = new ArrayList<Repository>();
        List<DocumentMK> ds = new ArrayList<DocumentMK>();
        
        initRepository(this.getClass(), rs, ds, 1, NOT_PROVIDED);
        
        Repository repository = rs.get(0);
        DocumentMK mk = ds.get(0);
        
        Session session = repository.login(ADMIN);
        session.logout();
        dispose(repository);
        mk.dispose(); // closes connection as well
    }

    protected void dispose(@Nonnull Repository repo) {
        AbstractRepositoryTest.dispose(checkNotNull(repo));
    }
    
    @After
    public void after() throws Exception {
        for (Repository repo : repos) {
            dispose(repo);
        }
        for (DocumentMK mk : mks) {
            mk.dispose();
        }
        dropDB(this.getClass());
    }

    /**
     * raise the exception passed into the provided Map
     * 
     * @param exceptions
     * @param log may be null. If valid Logger it will be logged
     * @throws Exception
     */
    static void raiseExceptions(@Nonnull final Map<String, Exception> exceptions, 
                                @Nullable final Logger log) throws Exception {
        if (exceptions != null) {
            for (Map.Entry<String, Exception> entry : exceptions.entrySet()) {
                if (log != null) {
                    log.error("Exception in thread {}", entry.getKey(), entry.getValue());
                }
                throw entry.getValue();
            }
        }
    }

    /**
     * <p> 
     * ensures that the cluster is aligned by running all the background operations
     * </p>
     *
     * @param mks the list of {@link DocumentMK} composing the cluster. Cannot be null.
     */
    static void alignCluster(@Nonnull final List<DocumentMK> mks) {
        // in a first round let all MKs run their background update
        for (DocumentMK mk : mks) {
            mk.getNodeStore().runBackgroundOperations();
        }
        String id = Utils.getIdFromPath("/");
        // in the second round each MK will pick up changes from the others
        for (DocumentMK mk : mks) {
            // invalidate root document to make sure background read
            // is forced to fetch the document from the store
            mk.getDocumentStore().invalidateCache(Collections.singleton(id));
            mk.getNodeStore().runBackgroundOperations();
        }
    }
    
    /**
     * set up the cluster connections. Same as {@link #setUpCluster(Class, List, List, int)}
     * providing {@link #NOT_PROVIDED} as {@code asyncDelay}
     * 
     * @param clazz class used for logging into Mongo itself
     * @param mks the list of mks to work on.
     * @param repos list of {@link Repository} created on each {@code mks}
     * @throws Exception
     */
    void setUpCluster(@Nonnull final Class<?> clazz, 
                             @Nonnull final List<DocumentMK> mks,
                             @Nonnull final List<Repository> repos) throws Exception {
        setUpCluster(clazz, mks, repos, NOT_PROVIDED);
    }

    /**
     * set up the cluster connections
     * 
     * @param clazz class used for logging into Mongo itself
     * @param mks the list of mks to work on
     * @param repos list of {@link Repository} created on each {@code mks}
     * @param asyncDelay the maximum delay for the cluster to sync with last revision. Use
     *            {@link #NOT_PROVIDED} for implementation default. Use {@code 0} for switching to
     *            manual and sync with {@link #alignCluster(List)}.
     * @throws Exception
     */
    void setUpCluster(@Nonnull final Class<?> clazz, 
                             @Nonnull final List<DocumentMK> mks,
                             @Nonnull final List<Repository> repos,
                             final int asyncDelay) throws Exception {
        for (int i = 0; i < NUM_CLUSTER_NODES; i++) {
            initRepository(clazz, repos, mks, i + 1, asyncDelay);
        }        
    }

    static MongoConnection createConnection(@Nonnull final Class<?> clazz) throws Exception {
        return OakMongoNSRepositoryStub.createConnection(
                checkNotNull(clazz).getSimpleName());
    }

    static void dropDB(@Nonnull final Class<?> clazz) throws Exception {
        MongoConnection con = createConnection(checkNotNull(clazz));
        try {
            con.getDB().dropDatabase();
        } finally {
            con.close();
        }
    }

    /**
     * initialise the repository
     * 
     * @param clazz the current class. Used for logging. Cannot be null.
     * @param repos list to which add the created repository. Cannot be null.
     * @param mks list to which add the created MK. Cannot be null.
     * @param clusterId the cluster ID to use. Must be greater than 0.
     * @param asyncDelay the async delay to set. For default use {@link #NOT_PROVIDED}
     * @throws Exception
     */
    protected void initRepository(@Nonnull final Class<?> clazz, 
                                  @Nonnull final List<Repository> repos, 
                                  @Nonnull final List<DocumentMK> mks,
                                  final int clusterId,
                                  final int asyncDelay) throws Exception {
        DocumentMK.Builder builder = new DocumentMK.Builder(); 
        builder.setMongoDB(createConnection(checkNotNull(clazz)).getDB());
        if (asyncDelay != NOT_PROVIDED) {
            builder.setAsyncDelay(asyncDelay);
        }
        builder.setClusterId(clusterId);
        
        DocumentMK mk = builder.open();
        Jcr j = getJcr(mk.getNodeStore());
        
        Set<IndexEditorProvider> ieps = additionalIndexEditorProviders();
        if (ieps != null) {
            for (IndexEditorProvider p : ieps) {
                j = j.with(p);
            }
        }
        
        if (isAsyncIndexing()) {
            j = j.withAsyncIndexing();
        }
        
        Repository repository = j.createRepository();
        
        checkNotNull(repos).add(repository);
        checkNotNull(mks).add(mk);
    }
    
    protected Jcr getJcr(@Nonnull NodeStore store) {
        Jcr j = new Jcr(checkNotNull(store));
        if (store instanceof Clusterable) {
            j.with((Clusterable) store);
        }
        return j;
    }
    
    /**
     * <p>
     * the default {@link #initRepository(Class, List, List, int, int)} uses this for registering
     * any additional {@link IndexEditorProvider}. Override and return all the provider you'd like
     * to have running other than the OOTB one.
     * </p>
     * 
     * <p>
     * the default implementation returns {@code null}
     * </p>
     * @return
     */
    protected Set<IndexEditorProvider> additionalIndexEditorProviders() {
        return null;
    }
    
    /**
     * override to change default behaviour. If {@code true} will enable the async indexing in the
     * cluster. Default is {@code false}
     * 
     * @return
     */
    protected boolean isAsyncIndexing() {
        return false;
    }
}
