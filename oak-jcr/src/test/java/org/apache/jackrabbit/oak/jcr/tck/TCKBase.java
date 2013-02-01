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

import java.lang.reflect.Field;
import java.util.Properties;

import org.apache.jackrabbit.oak.jcr.OakMongoMKRepositoryStub;
import org.apache.jackrabbit.oak.jcr.OakRepositoryStub;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.RepositoryHelper;
import org.apache.jackrabbit.test.RepositoryHelperPool;
import org.apache.jackrabbit.test.RepositoryStub;

import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Base class for TCK tests.
 */
public abstract class TCKBase extends TestSuite {

    public TCKBase(String name) {
        super(name);
        addTest(new Setup(OakRepositoryStub.class.getName()));
        addTests();
        // OAK-588: CI builds take too long with MongoMK
        // -> disabled for now
        if (false && OakMongoMKRepositoryStub.isMongoDBAvailable()) {
            addTest(new Setup(OakMongoMKRepositoryStub.class.getName()));
            addTests();
        }
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
            Field poolField = AbstractJCRTest.class.getDeclaredField("HELPER_POOL");
            poolField.setAccessible(true);
            RepositoryHelperPool helperPool = (RepositoryHelperPool) poolField.get(null);
            helperPool.borrowHelper();
            Properties props = new Properties();
            props.load(getClass().getClassLoader().getResourceAsStream(RepositoryStub.STUB_IMPL_PROPS));
            props.put(RepositoryStub.PROP_STUB_IMPL_CLASS, stubClass);
            helperPool.returnHelper(new RepositoryHelper(props));
        }
    }
}
