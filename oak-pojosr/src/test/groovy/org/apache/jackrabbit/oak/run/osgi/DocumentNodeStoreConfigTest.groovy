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

package org.apache.jackrabbit.oak.run.osgi

import org.apache.felix.connect.launch.PojoServiceRegistry
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobStore
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection
import org.apache.jackrabbit.oak.spi.blob.BlobStore
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore
import org.apache.jackrabbit.oak.spi.state.NodeStore
import org.h2.jdbcx.JdbcDataSource
import org.junit.After
import org.junit.Test
import org.osgi.framework.ServiceRegistration

import javax.sql.DataSource
import java.sql.Connection
import java.sql.ResultSet
import java.util.concurrent.TimeUnit

import static org.junit.Assume.assumeTrue

class DocumentNodeStoreConfigTest extends AbstractRepositoryFactoryTest {
    private PojoServiceRegistry registry
    private MongoConnection mongoConn = MongoUtils.getConnection()

    @Test
    public void testRDBDocumentStore() throws Exception {
        registry = repositoryFactory.initializeServiceRegistry(config)

        //1. Register the DataSource as a service
        DataSource ds = createDS("jdbc:h2:mem:testRDB;DB_CLOSE_DELAY=-1")
        registry.registerService(DataSource.class.name, ds, ['datasource.name': 'oak'] as Hashtable)

        //2. Create config for DocumentNodeStore with RDB enabled
        createConfig([
                'org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService': [
                        documentStoreType: 'RDB'
                ]
        ])

        DocumentNodeStore ns = getServiceWithWait(NodeStore.class)

        //3. Check that DS contains tables from both RDBBlobStore and RDBDocumentStore
        assert getExistingTables(ds).containsAll(['NODES', 'DATASTORE_META'])

        //4. Check that only one cluster node was instantiated
        assert getIdsOfClusterNodes(ds).size() == 1
    }

    @Test
    public void testRDBDocumentStoreRestart() throws Exception {
        registry = repositoryFactory.initializeServiceRegistry(config)

        //1. Register the DataSource as a service
        DataSource ds = createDS("jdbc:h2:mem:testRDBrestart;DB_CLOSE_DELAY=-1")
        ServiceRegistration srds = registry.registerService(DataSource.class.name, ds, ['datasource.name': 'oak'] as Hashtable)

        //2. Create config for DocumentNodeStore with RDB enabled
        createConfig([
                'org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService': [
                        documentStoreType: 'RDB'
                ]
        ])

        DocumentNodeStore ns = getServiceWithWait(NodeStore.class)

        //3. Shut down ds
        srds.unregister();
        TimeUnit.MILLISECONDS.sleep(500);
        assertNoService(NodeStore.class)

        //4. Restart ds, service should still be down
        srds = registry.registerService(DataSource.class.name, ds, ['datasource.name': 'oak'] as Hashtable)
        assertNoService(NodeStore.class)
    }

    @Test
    public void testRDBDocumentStoreLateDataSource() throws Exception {
        registry = repositoryFactory.initializeServiceRegistry(config)

        //1. Create config for DocumentNodeStore with RDB enabled
        createConfig([
                'org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService': [
                        documentStoreType: 'RDB'
                ]
        ])

        //2. Register the DataSource as a service
        DataSource ds = createDS("jdbc:h2:mem:testRDBlateds;DB_CLOSE_DELAY=-1")
        registry.registerService(DataSource.class.name, ds, ['datasource.name': 'oak'] as Hashtable)

        DocumentNodeStore ns = getServiceWithWait(NodeStore.class)

        //3. Check that DS contains tables from both RDBBlobStore and RDBDocumentStore
        assert getExistingTables(ds).containsAll(['NODES', 'DATASTORE_META'])

        //4. Check that only one cluster node was instantiated
        assert getIdsOfClusterNodes(ds).size() == 1
    }

    @Test
    public void testRDBDocumentStore_CustomBlobDataSource() throws Exception {
        registry = repositoryFactory.initializeServiceRegistry(config)

        //1. Register the DataSource as a service
        DataSource ds1 = createDS("jdbc:h2:mem:testRDB1;DB_CLOSE_DELAY=-1")
        registry.registerService(DataSource.class.name, ds1, ['datasource.name': 'oak'] as Hashtable)

        //2. Create config for DocumentNodeStore with RDB enabled
        createConfig([
                'org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService': [
                        documentStoreType      : 'RDB',
                        'blobDataSource.target': '(datasource.name=oak-blob)',
                ]
        ])

        DataSource ds2 = createDS("jdbc:h2:mem:testRDB2;DB_CLOSE_DELAY=-1")
        registry.registerService(DataSource.class.name, ds2, ['datasource.name': 'oak-blob'] as Hashtable)

        DocumentNodeStore ns = getServiceWithWait(NodeStore.class)

        //3. Check that DS contains tables from RDBBlobStore and RDBDocumentStore
        //in there respective DataStores
        List<String> ds1Tables = getExistingTables(ds1)
        List<String> ds2Tables = getExistingTables(ds2)

        //DS1 should contain RDBDocumentStore tables
        assert ds1Tables.contains('NODES')
        assert !ds1Tables.contains('DATASTORE_META')

        //DS2 should contain only RDBBlobStore tables
        assert !ds2Tables.contains('NODES')
        assert ds2Tables.contains('DATASTORE_META')

        //4. Check that only one cluster node was instantiated
        assert getIdsOfClusterNodes(ds1).size() == 1
    }

    @Test
    public void testRDBDocumentStore_CustomBlobStore() throws Exception {
        registry = repositoryFactory.initializeServiceRegistry(config)

        //1. Register the DataSource as a service
        DataSource ds1 = createDS("jdbc:h2:mem:testRDB3;DB_CLOSE_DELAY=-1")
        ServiceRegistration sdsds = registry.registerService(DataSource.class.name, ds1, ['datasource.name': 'oak'] as Hashtable)

        DataSource ds2 = createDS("jdbc:h2:mem:testRDB3b;DB_CLOSE_DELAY=-1")
        ServiceRegistration sdsbs = registry.registerService(DataSource.class.name, ds2, ['datasource.name': 'oak-blob'] as Hashtable)

        //2. Create config for DocumentNodeStore with RDB enabled
        // (supply blobDataSource which should be ignored because customBlob takes precedence)
        createConfig([
                'org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService': [
                        documentStoreType: 'RDB',
                        'blobDataSource.target': '(datasource.name=oak-blob)',
                        customBlobStore  : true,
                ]
        ])

        registry.registerService(BlobStore.class.name, new MemoryBlobStore(), null)

        DocumentNodeStore ns = getServiceWithWait(NodeStore.class)

        //3. Check that DS1 contains tables only from both RDBDocumentStore
        List<String> ds1Tables = getExistingTables(ds1)

        //DS1 should contain RDBDocumentStore tables only
        assert ds1Tables.contains('NODES')
        assert !ds1Tables.contains('DATASTORE_META')

        //4. Check that DS2 is empty
        List<String> ds2Tables = getExistingTables(ds2)
        assert !ds2Tables.contains('NODES')
        assert !ds2Tables.contains('DATASTORE_META')

        //5. Check that only one cluster node was instantiated
        assert getIdsOfClusterNodes(ds1).size() == 1

        //6. Unregister the data sources to test resilience wrt
        //multiple deregistrations (OAK-3383)
        sdsds.unregister();
        sdsbs.unregister();
    }

    @Test
    public void testMongoDocumentStore_CustomBlobStore() throws Exception {
        mongoCheck()

        registry = repositoryFactory.initializeServiceRegistry(config)
        createConfig([
                'org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService': [
                        mongouri       : MongoUtils.mongoURI,
                        db             : MongoUtils.mongoDB,
                        customBlobStore: true
                ]
        ])

        registry.registerService(BlobStore.class.name, new MemoryBlobStore(), null)

        DocumentNodeStore ns = getServiceWithWait(NodeStore.class)

        Collection<String> colNames = getCollectionNames()
        assert colNames.containsAll(['NODES'])
        assert !colNames.contains(['BLOBS'])
    }

    @Test
    public void testMongoDocumentStore() throws Exception {
        mongoCheck()

        registry = repositoryFactory.initializeServiceRegistry(config)
        createConfig([
                'org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService': [
                        mongouri: MongoUtils.mongoURI,
                        db      : MongoUtils.mongoDB,
                        blobCacheSize      : 1024,
                ]
        ])

        DocumentNodeStore ns = getServiceWithWait(NodeStore.class)

        Collection<String> colNames = getCollectionNames()
        assert colNames.containsAll(['NODES', "BLOBS"])

        assert 1024*1024*1024 == ((MongoBlobStore)ns.blobStore).blobCacheSize
    }

    @Override
    protected PojoServiceRegistry getRegistry() {
        return registry
    }

    @After
    public void tearDown() {
        if (mongoConn) {
            MongoUtils.dropCollections(mongoConn.DB)
        }
    }

    private mongoCheck() {
        //Somehow in Groovy assumeNotNull cause issue as Groovy probably
        //does away with null array causing a NPE
        assumeTrue(mongoConn != null)
    }

    private Collection<String> getCollectionNames() {
        return mongoConn.DB.getCollectionNames().collect { it.toUpperCase() }
    }

    private List<String> getExistingTables(DataSource ds) {
        Connection con = ds.connection
        List<String> existing = []
        try {
            ResultSet rs = con.metaData.getTables(null, null, "%", null)
            while (rs.next()) {
                existing << rs.toRowResult()['TABLE_NAME'].toUpperCase()
            }
        } finally {
            con.close()
        }
        return existing
    }

    private List<String> getIdsOfClusterNodes(DataSource ds) {
        Connection con = ds.connection
        List<String> entries = []
        try {
            ResultSet rs = con.prepareStatement("SELECT ID FROM CLUSTERNODES").executeQuery()
            while (rs.next()) {
                entries << rs.get(1)
            }
        } finally {
            con.close()
        }
        return entries
    }
    
    private DataSource createDS(String url) {
        DataSource ds = new JdbcDataSource()
        ds.url = url
        return ds
    }
}
