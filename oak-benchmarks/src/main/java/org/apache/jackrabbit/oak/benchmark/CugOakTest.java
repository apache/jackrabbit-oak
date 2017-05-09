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
package org.apache.jackrabbit.oak.benchmark;

import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.Repository;
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;

/**
 * Test the effect of multiple authorization configurations on the general read
 * operations.
 *
 * TODO: setup configured number of cugs.
 */
public class CugOakTest extends CugTest {

    private ContentRepository contentRepository;
    private ContentSession cs;
    private Subject subject;

    protected CugOakTest(boolean runAsAdmin, int itemsToRead, boolean singleSession, @Nonnull List<String> supportedPaths, boolean reverseOrder) {
        super(runAsAdmin, itemsToRead, singleSession, supportedPaths, reverseOrder);
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    Jcr jcr = new Jcr(oak).with(createSecurityProvider());
                    contentRepository = jcr.createContentRepository();
                    return jcr;
                }
            });
        } else {
            throw new IllegalArgumentException("Fixture " + fixture + " not supported for this benchmark.");
        }
    }

    @Override
    protected void beforeSuite() throws Exception {
        super.beforeSuite();
        Credentials creds = (runAsAdmin) ? getCredentials() : new GuestCredentials();
        cs = contentRepository.login(creds, null);
        subject = new Subject(true, cs.getAuthInfo().getPrincipals(), Collections.emptySet(), Collections.emptySet());
    }

    @Override
    protected void afterSuite() throws Exception {
        super.afterSuite();
        cs.close();
    }

    @Override
    protected void runTest() throws Exception {
        boolean logout = false;
        ContentSession readSession;
        if (singleSession) {
            readSession = cs;
        } else {
            readSession = Subject.doAs(subject, new PrivilegedAction<ContentSession>() {
                @Override
                public ContentSession run() {
                    try {
                        return contentRepository.login(null, null);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            logout = true;
        }
        Root root = readSession.getLatestRoot();
        try {
            int nodeCnt = 0;
            int propertyCnt = 0;
            int noAccess = 0;
            int size = allPaths.size();
            long start = System.currentTimeMillis();
            for (int i = 0; i < itemsToRead; i++) {
                double rand = size * Math.random();
                int index = (int) Math.floor(rand);
                String path = allPaths.get(index);
                TreeLocation treeLocation = TreeLocation.create(root, path);
                if (treeLocation.exists()) {
                    PropertyState ps = treeLocation.getProperty();
                    if (ps != null) {
                        propertyCnt++;
                    } else {
                        nodeCnt++;
                    }
                } else {
                    noAccess++;
                }
            }
            long end = System.currentTimeMillis();
            if (doReport) {
                System.out.println("ContentSession " + cs.getAuthInfo().getUserID() + " reading " + (itemsToRead - noAccess) + " (Tree: " + nodeCnt + "; PropertyState: " + propertyCnt + ") completed in " + (end - start));
            }
        } finally {
            if (logout) {
                readSession.close();
            }
        }
    }
}