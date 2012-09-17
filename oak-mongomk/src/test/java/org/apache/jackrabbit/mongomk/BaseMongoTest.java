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
package org.apache.jackrabbit.mongomk;

import java.io.InputStream;
import java.util.Properties;

import org.apache.jackrabbit.mongomk.MongoConnection;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;


/**
 * Base class for {@code MongoDB} tests.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
@SuppressWarnings("javadoc")
public class BaseMongoTest {

    public static MongoConnection mongoConnection;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        BasicConfigurator.configure();
        createDefaultMongoConnection();
        MongoAssert.setMongoConnection(mongoConnection);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        dropDefaultDatabase();
    }

    private static void createDefaultMongoConnection() throws Exception {
        InputStream is = BaseMongoTest.class.getResourceAsStream("/config.cfg");
        Properties properties = new Properties();
        properties.load(is);

        String host = properties.getProperty("host");
        int port = Integer.parseInt(properties.getProperty("port"));
        String database = properties.getProperty("db");

        mongoConnection = new MongoConnection(host, port, database);
    }

    private static void dropDefaultDatabase() {
        mongoConnection.getDB().dropDatabase();
    }

    @Before
    public void setUp() throws Exception {
        MongoUtil.initDatabase(mongoConnection);
    }

    @After
    public void tearDown() throws Exception {
        MongoUtil.clearDatabase(mongoConnection);
    }
}
