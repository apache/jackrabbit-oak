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
package org.apache.jackrabbit.oak.jcr.cluster;

import static org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest.dispose;

import java.util.Iterator;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import junit.framework.Assert;
import org.apache.jackrabbit.oak.NodeStoreFixtures;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.After;
import org.junit.Before;

/**
 * A base class for DocumentMK cluster tests.
 */
public class AbstractClusterTest {

    protected NodeStoreFixture fixture = getFixture();
    protected NodeStore ns1, ns2;
    protected Whiteboard w1, w2;
    protected Repository r1, r2;
    protected Session s1, s2;
    
    protected NodeStoreFixture getFixture() {
        return NodeStoreFixtures.DOCUMENT_NS;
    }
    
    @After
    public void logout() {
        if (s1 != null) {
            s1.logout();
            s1 = null;
        }
        if (s2 != null) {
            s2.logout();
            s2 = null;
        }
        r1 = dispose(r1);
        r2 = dispose(r2);
        if (ns1 != null) {
            fixture.dispose(ns1);
        }
        if (ns2 != null) {
            fixture.dispose(ns2);
        }
    }

    protected void prepareTestData(Session s) throws RepositoryException {
    }

    protected Jcr customize(Jcr jcr) {
        return jcr;
    }

    @Before
    public void login() throws RepositoryException {
        ns1 = fixture.createNodeStore(1);
        if (ns1 == null) {
            return;
        }
        Oak oak1 = new Oak(ns1);
        w1 = oak1.getWhiteboard();
        r1  = customize(new Jcr(oak1)).createRepository();
        s1 = r1.login(new SimpleCredentials("admin", "admin".toCharArray()));
        prepareTestData(s1);
        if (ns1 instanceof DocumentNodeStore) {
            // make sure initial repository data is visible to
            // other cluster nodes initialized later
            ((DocumentNodeStore) ns1).runBackgroundOperations();
        }
        ns2 = fixture.createNodeStore(2);
        Oak oak2 = new Oak(ns2);
        w2 = oak2.getWhiteboard();
        r2  = customize(new Jcr(oak2)).createRepository();
        s2 = r2.login(new SimpleCredentials("admin", "admin".toCharArray()));
    }
    
    static Iterable<Integer> seconds(final int max) {
        return new Iterable<Integer>() {

            @Override
            public Iterator<Integer> iterator() {
                return new Iterator<Integer>() {
                    
                    long start = System.currentTimeMillis();
                    int x;

                    @Override
                    public boolean hasNext() {
                        long time = System.currentTimeMillis() - start;
                        if (x > 0 && time >= (max * 1000)) {
                            Assert.fail("Retry loop timed out after " + x + 
                                    " repetitions and " + time + " milliseconds");
                        }
                        return true;
                    }

                    @Override
                    public Integer next() {
                        hasNext();
                        if (x > 0) {
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }
                        return x++;
                    }
                    
                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                    
                };
            }
            
        };
    }
    
}
