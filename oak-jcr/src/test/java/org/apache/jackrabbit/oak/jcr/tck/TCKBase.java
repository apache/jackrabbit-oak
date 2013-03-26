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
package org.apache.jackrabbit.oak.jcr.tck;

import java.util.Properties;

import org.apache.jackrabbit.oak.jcr.OakRepositoryStub;
import org.apache.jackrabbit.oak.jcr.OakSegmentMKRepositoryStub;
import org.apache.jackrabbit.test.RepositoryHelper;
import org.apache.jackrabbit.test.RepositoryHelperPool;
import org.apache.jackrabbit.test.RepositoryHelperPoolImpl;
import org.apache.jackrabbit.test.RepositoryStub;
import org.slf4j.bridge.SLF4JBridgeHandler;

import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Base class for TCK tests.
 */
public abstract class TCKBase extends TestSuite {

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    public TCKBase(String name) {
        super(name);
        addTest(new Setup(OakRepositoryStub.class.getName()));
        addTests();
        if (OakSegmentMKRepositoryStub.isAvailable()) {
            addTest(new Setup(OakSegmentMKRepositoryStub.class.getName()));
            addTests();
        }
        // OAK-588: CI builds take too long with MongoMK
        // -> disabled for now
//        if (OakMongoMKRepositoryStub.isMongoDBAvailable()) {
//            addTest(new Setup(OakMongoMKRepositoryStub.class.getName()));
//            addTests();
//        }
    }

    abstract protected void addTests();

    /**
     * Setup test class to replace the RepositoryHelper. This is quite a hack
     * be cause the existing TCK tests do not take parameters.
     */
    public static class Setup extends TestCase {

        private final String stubClass;

        public Setup(String stubClass) {
            super("testSetup");
            this.stubClass = stubClass;
        }

        public void testSetup() throws Exception {
            // replace the existing helper with our parametrized version
            RepositoryHelperPool helperPool = RepositoryHelperPoolImpl.getInstance();
            // drain helpers
            helperPool.borrowHelpers();
            // replace with our own stub
            Properties props = new Properties();
            props.load(getClass().getClassLoader().getResourceAsStream(RepositoryStub.STUB_IMPL_PROPS));
            props.put(RepositoryStub.PROP_STUB_IMPL_CLASS, stubClass);
            helperPool.returnHelper(new RepositoryHelper(props));
        }
    }
}
