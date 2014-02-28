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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.jackrabbit.oak.jcr.OakDocumentRDBRepositoryStub;
import org.apache.jackrabbit.oak.jcr.OakMongoNSRepositoryStub;
import org.apache.jackrabbit.oak.jcr.OakTarMKRepositoryStub;
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
        Setup.wrap(this, OakTarMKRepositoryStub.class.getName());
        if (OakMongoNSRepositoryStub.isMongoDBAvailable()) {
            Setup.wrap(this, OakMongoNSRepositoryStub.class.getName());
        }
        if (OakDocumentRDBRepositoryStub.isAvailable()) {
            Setup.wrap(this, OakDocumentRDBRepositoryStub.class.getName());
        }
    }

    abstract protected void addTests();

    /**
     * Setup test class to replace the RepositoryHelper. This is quite a hack
     * because the existing TCK tests do not take parameters.
     */
    public static class Setup extends TestCase {

        private static Map<String, RepositoryHelper> HELPERS = new HashMap<String, RepositoryHelper>();

        private final String stubClass;

        private List<RepositoryHelper> previous = new ArrayList<RepositoryHelper>();

        public static void wrap(TCKBase test, String stubClass) {
            Setup setup = new Setup(stubClass);
            test.addTest(setup);
            test.addTests();
            test.addTest(setup.getTearDown());
        }

        public Setup(String stubClass) {
            super("testSetup");
            this.stubClass = stubClass;
        }

        public void testSetup() throws Exception {
            // replace the existing helper with our parametrized version
            RepositoryHelperPool helperPool = RepositoryHelperPoolImpl.getInstance();
            // drain helpers
            previous.addAll(Arrays.asList(helperPool.borrowHelpers()));
            // replace with our own stub
            helperPool.returnHelper(getRepositoryHelper());
        }

        private RepositoryHelper getRepositoryHelper() throws Exception {
            RepositoryHelper helper = HELPERS.get(stubClass);
            if (helper == null) {
                Properties props = new Properties();
                props.load(getClass().getClassLoader().getResourceAsStream(RepositoryStub.STUB_IMPL_PROPS));
                props.put(RepositoryStub.PROP_STUB_IMPL_CLASS, stubClass);
                helper = new RepositoryHelper(props);
                HELPERS.put(stubClass, helper);
            }
            return helper;
        }

        TestCase getTearDown() {
            return new TearDown(previous);
        }
    }

    public static class TearDown extends TestCase {

        /**
         * The repository helpers to restore
         */
        private List<RepositoryHelper> helpers;

        public TearDown(List<RepositoryHelper> helpers) {
            super("testTearDown");
            this.helpers = helpers;
        }

        public void testTearDown() throws Exception {
            // restore previous helpers
            RepositoryHelperPool helperPool = RepositoryHelperPoolImpl.getInstance();
            helperPool.borrowHelpers();
            for (RepositoryHelper helper : helpers) {
                helperPool.returnHelper(helper);
            }
        }
    }
}
