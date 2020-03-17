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

package org.apache.jackrabbit.oak.segment.standby.store;

import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CommunicationObserverTest {

    private static class TestCommunicationObserver extends CommunicationObserver {

        private final List<CommunicationPartnerMBean> communicationPartners = new ArrayList<>();

        TestCommunicationObserver(String id, int maxClientMBeans) {
            super(id, maxClientMBeans);
        }

        @Override
        void registerCommunicationPartner(CommunicationPartnerMBean m) throws Exception {
            super.registerCommunicationPartner(m);
            communicationPartners.add(m);
        }

        @Override
        void unregisterCommunicationPartner(CommunicationPartnerMBean m) throws Exception {
            communicationPartners.remove(m);
            super.unregisterCommunicationPartner(m);
        }

    }

    private TestCommunicationObserver observer;

    @Before
    public void before() {
        observer = new TestCommunicationObserver("test", 10);
    }

    @After
    public void after() {
        observer.unregister();
        observer = null;
    }

    @Test
    public void shouldExposeTheProvidedID() throws Exception {
        assertEquals("test", observer.getID());
    }

    @Test
    public void shouldRegisterObservablePartnerMBean() throws Exception {
        observer.gotMessageFrom("client", "request", "127.0.0.1", 8080);
        assertEquals(1, observer.communicationPartners.size());
    }

    @Test
    public void shouldNotKeepManyObservablePartnerMBeans() throws Exception {
        for (int i = 0; i < 20; i++) {
            observer.gotMessageFrom(randomUUID().toString(), "request", "127.0.0.1", 8080);
        }
        assertEquals(10, observer.communicationPartners.size());
    }

    @Test
    public void observablePartnerMBeanShouldExposeClientName() throws Exception {
        observer.gotMessageFrom("client", "request", "127.0.0.1", 8080);
        assertEquals("client", observer.communicationPartners.get(0).getName());
    }

    @Test
    public void observablePartnerMBeanShouldExposeAddress() throws Exception {
        observer.gotMessageFrom("client", "request", "127.0.0.1", 8080);
        assertEquals("127.0.0.1", observer.communicationPartners.get(0).getRemoteAddress());
    }

    @Test
    public void observablePartnerMBeanShouldExposePort() throws Exception {
        observer.gotMessageFrom("client", "request", "127.0.0.1", 8080);
        assertEquals(8080, observer.communicationPartners.get(0).getRemotePort());
    }

    @Test
    public void observablePartnerMBeanShouldExposeLastRequest() throws Exception {
        observer.gotMessageFrom("client", "request", "127.0.0.1", 8080);
        assertEquals("request", observer.communicationPartners.get(0).getLastRequest());
    }

    @Test
    public void observablePartnerMBeanShouldUpdateLastRequest() throws Exception {
        observer.gotMessageFrom("client", "before", "127.0.0.1", 8080);
        assertEquals("before", observer.communicationPartners.get(0).getLastRequest());
        observer.gotMessageFrom("client", "after", "127.0.0.1", 8080);
        assertEquals("after", observer.communicationPartners.get(0).getLastRequest());
    }

    @Test
    public void observablePartnerMBeanShouldExposeLastSeen() throws Exception {
        observer.gotMessageFrom("client", "request", "127.0.0.1", 8080);
        assertNotNull(observer.communicationPartners.get(0).getLastSeen());
    }

    @Test
    public void observablePartnerMBeanShouldUpdateLastSeen() throws Exception {
        observer.gotMessageFrom("client", "request", "127.0.0.1", 8080);
        Date before = observer.communicationPartners.get(0).getLastSeen();
        observer.gotMessageFrom("client", "request", "127.0.0.1", 8080);
        Date after = observer.communicationPartners.get(0).getLastSeen();
        assertNotSame(before, after);
    }

    @Test
    public void observablePartnerMBeanShouldExposeLastSeenTimestamp() throws Exception {
        observer.gotMessageFrom("client", "request", "127.0.0.1", 8080);
        assertNotNull(observer.communicationPartners.get(0).getLastSeenTimestamp());
    }

    @Test
    public void observablePartnerMBeanShouldExposeSentSegments() throws Exception {
        observer.gotMessageFrom("client", "request", "127.0.0.1", 8080);
        observer.didSendSegmentBytes("client", 100);
        assertEquals(1, observer.communicationPartners.get(0).getTransferredSegments());
    }

    @Test
    public void observablePartnerMBeanShouldExposeSentSegmentsSize() throws Exception {
        observer.gotMessageFrom("client", "request", "127.0.0.1", 8080);
        observer.didSendSegmentBytes("client", 100);
        assertEquals(100, observer.communicationPartners.get(0).getTransferredSegmentBytes());
    }

    @Test
    public void observablePartnerMBeanShouldExposeSentBinaries() throws Exception {
        observer.gotMessageFrom("client", "request", "127.0.0.1", 8080);
        observer.didSendBinariesBytes("client", 100);
        assertEquals(1, observer.communicationPartners.get(0).getTransferredBinaries());
    }

    @Test
    public void observablePartnerMBeanShouldExposeSentBinariesSize() throws Exception {
        observer.gotMessageFrom("client", "request", "127.0.0.1", 8080);
        observer.didSendBinariesBytes("client", 100);
        assertEquals(100, observer.communicationPartners.get(0).getTransferredBinariesBytes());
    }

}
