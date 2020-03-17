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
package org.apache.jackrabbit.oak.commons.jmx;

import java.lang.management.ManagementFactory;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.jackrabbit.oak.api.jmx.Description;
import org.apache.jackrabbit.oak.api.jmx.Impact;
import org.apache.jackrabbit.oak.api.jmx.ImpactOption;
import org.apache.jackrabbit.oak.api.jmx.Name;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

@SuppressWarnings("ALL")
public class AnnotatedStandardMBeanTest {
    protected static ObjectName objectName;

    protected static MBeanServer server;

    @BeforeClass
    public static void init() throws Exception {
        AnnotatedStandardMBean object = new AnnotatedStandardMBean(new Foo(),
            FooMBean.class);

        objectName = new ObjectName("abc:TYPE=Test");

        server = ManagementFactory.getPlatformMBeanServer();
        server.registerMBean(object, objectName);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        server.unregisterMBean(objectName);
    }

    @Test
    public void test() throws Exception {
        MBeanInfo info = server.getMBeanInfo(objectName);

        assertEquals("MBean desc.", info.getDescription());

        MBeanAttributeInfo a0 = findAttribute(info, "Getter");
        assertEquals("getter", a0.getDescription());

        MBeanAttributeInfo a1 = findAttribute(info, "It");
        assertEquals("is", a1.getDescription());

        MBeanAttributeInfo a2 = findAttribute(info, "Setter");
        assertEquals("setter", a2.getDescription());

        MBeanOperationInfo op0 = info.getOperations()[0];
        assertEquals("run", op0.getDescription());
        assertEquals(MBeanOperationInfo.INFO, op0.getImpact());

        MBeanParameterInfo p0 = op0.getSignature()[0];
        assertEquals("timeout", p0.getName());
        assertEquals("how long?", p0.getDescription());
    }

    private MBeanAttributeInfo findAttribute(MBeanInfo info, String name) {
        for (MBeanAttributeInfo a : info.getAttributes()) {
            if (a.getName().equals(name)) return a;
        }

        return null;
    }

    @Description("MBean desc.")
    public interface FooMBean {
        @Description("getter")
        String getGetter();

        @Description("is")
        boolean isIt();

        @Description("setter")
        void setSetter(long s);

        @Description("run")
        @Impact(ImpactOption.INFO)
        void run(@Name("timeout") @Description("how long?") long timeout);
    }

    public static class Foo implements FooMBean {

        public String getGetter() {
            return null;
        }

        public boolean isIt() {
            return false;
        }

        public void setSetter(long s) {
        }

        public void run(long timeout) {
        }
    }
}
