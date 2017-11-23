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
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_COUNTER;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_INCREMENT;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.MIX_ATOMIC_COUNTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.apache.jackrabbit.oak.NodeStoreFixtures;
import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.BeforeClass;
import org.junit.Test;

public class AtomicCounterIT extends AbstractRepositoryTest {
    private static final Set<Fixture> FIXTURES = FixturesHelper.getFixtures();
        
    public AtomicCounterIT(NodeStoreFixture fixture) {
        super(fixture);
    }

    @BeforeClass
    public static void assumptions() {
        assumeTrue(FIXTURES.contains(Fixture.SEGMENT_TAR));
    }

    @Test
    public void concurrentSegmentIncrements() throws RepositoryException, InterruptedException, 
                                                     ExecutionException {
        // ensuring the run only on allowed fix
        assumeTrue(NodeStoreFixtures.SEGMENT_TAR.equals(fixture));
        
        // setting-up
        Session session = getAdminSession();
        
        try {
            Node counter = session.getRootNode().addNode("counter");
            counter.addMixin(MIX_ATOMIC_COUNTER);
            session.save();
            
            final AtomicLong expected = new AtomicLong(0);
            final String counterPath = counter.getPath();
            final Random rnd = new Random(11);
            
            // ensuring initial state
            assertEquals(expected.get(), counter.getProperty(PROP_COUNTER).getLong());
            
            List<ListenableFutureTask<Void>> tasks = Lists.newArrayList();
            for (int t = 0; t < 100; t++) {
                tasks.add(updateCounter(counterPath, rnd.nextInt(10) + 1, expected));
            }
            Futures.allAsList(tasks).get();
            
            session.refresh(false);
            assertEquals(expected.get(), 
                session.getNode(counterPath).getProperty(PROP_COUNTER).getLong());
        } finally {
            session.logout();
        }
    }
    
    private ListenableFutureTask<Void> updateCounter(@Nonnull final String counterPath,
                                                     final long delta,
                                                     @Nonnull final AtomicLong expected) {
        checkNotNull(counterPath);
        checkNotNull(expected);
        
        ListenableFutureTask<Void> task = ListenableFutureTask.create(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                Session session = createAdminSession();
                try {
                    Node c = session.getNode(counterPath);
                    c.setProperty(PROP_INCREMENT, delta);
                    expected.addAndGet(delta);
                    session.save();
                } finally {
                    session.logout();
                }
                return null;
            }
        });
        
        new Thread(task).start();
        return task;
    }

    @Override
    protected Jcr initJcr(Jcr jcr) {
        return super.initJcr(jcr).withAtomicCounter();
    }
}
