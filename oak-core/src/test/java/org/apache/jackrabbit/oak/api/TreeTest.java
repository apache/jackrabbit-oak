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
package org.apache.jackrabbit.oak.api;

import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandler;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.OakAssert.assertSequence;
import static org.junit.Assert.assertEquals;

/**
 * Contains tests related to {@link Tree}
 */
public class TreeTest {

    private ContentRepository repository;

    @Before
    public void setUp() {
        repository = new Oak()
            .with(new ConflictValidator())
            .with(new AnnotatingConflictHandler())
            .createContentRepository();
    }

    @After
    public void tearDown() {
        repository = null;
    }

    @Test
    public void orderBefore() throws Exception {
        ContentSession s = repository.login(null, null);
        try {
            Root r = s.getLatestRoot();
            Tree t = r.getTree("/");
            t.addChild("node1");
            t.addChild("node2");
            t.addChild("node3");
            r.commit();
            t = r.getTree("/");
            t.getChild("node1").orderBefore("node2");
            t.getChild("node3").orderBefore(null);
            assertSequence(t.getChildren(), "node1", "node2", "node3");
            r.commit();
            // check again after commit
            t = r.getTree("/");
            assertSequence(t.getChildren(), "node1", "node2", "node3");

            t.getChild("node3").orderBefore("node2");
            assertSequence(t.getChildren(), "node1", "node3", "node2");
            r.commit();
            t = r.getTree("/");
            assertSequence(t.getChildren(), "node1", "node3", "node2");

            t.getChild("node1").orderBefore(null);
            assertSequence(t.getChildren(), "node3", "node2", "node1");
            r.commit();
            t = r.getTree("/");
            assertSequence(t.getChildren(), "node3", "node2", "node1");

            // TODO :childOrder property invisible?
            //assertEquals("must not have any properties", 0, t.getPropertyCount());
        } finally {
            s.close();
        }
    }

    @Test
    public void concurrentOrderBefore() throws Exception {
        ContentSession s1 = repository.login(null, null);
        try {
            Root r1 = s1.getLatestRoot();
            Tree t1 = r1.getTree("/");
            t1.addChild("node1");
            t1.addChild("node2");
            t1.addChild("node3");
            r1.commit();
            t1 = r1.getTree("/");

            ContentSession s2 = repository.login(null, null);
            try {
                Root r2 = s2.getLatestRoot();
                Tree t2 = r2.getTree("/");

                t1.getChild("node2").orderBefore("node1");
                t1.getChild("node3").orderBefore(null);
                r1.commit();
                t1 = r1.getTree("/");
                assertSequence(t1.getChildren(), "node2", "node1", "node3");

                t2.getChild("node3").orderBefore("node1");
                t2.getChild("node2").orderBefore(null);
                r2.commit();
                t2 = r2.getTree("/");
                // other session wins
                assertSequence(t2.getChildren(), "node2", "node1", "node3");

                // try again on current root
                t2.getChild("node3").orderBefore("node1");
                t2.getChild("node2").orderBefore(null);
                r2.commit();
                t2 = r2.getTree("/");
                assertSequence(t2.getChildren(), "node3", "node1", "node2");

            } finally {
                s2.close();
            }
        } finally {
            s1.close();
        }
    }

    @Test
    public void concurrentOrderBeforeWithAdd() throws Exception {
        ContentSession s1 = repository.login(null, null);
        try {
            Root r1 = s1.getLatestRoot();
            Tree t1 = r1.getTree("/");
            t1.addChild("node1");
            t1.addChild("node2");
            t1.addChild("node3");
            r1.commit();
            t1 = r1.getTree("/");

            ContentSession s2 = repository.login(null, null);
            try {
                Root r2 = s2.getLatestRoot();
                Tree t2 = r2.getTree("/");

                t1.getChild("node2").orderBefore("node1");
                t1.getChild("node3").orderBefore(null);
                t1.addChild("node4");
                r1.commit();
                t1 = r1.getTree("/");
                assertSequence(t1.getChildren(), "node2", "node1", "node3", "node4");

                t2.getChild("node3").orderBefore("node1");
                r2.commit();
                t2 = r2.getTree("/");
                // other session wins
                assertSequence(t2.getChildren(), "node2", "node1", "node3", "node4");

                // try again on current root
                t2.getChild("node3").orderBefore("node1");
                r2.commit();
                t2 = r2.getTree("/");
                assertSequence(t2.getChildren(), "node2", "node3", "node1", "node4");

            } finally {
                s2.close();
            }
        } finally {
            s1.close();
        }
    }

    @Test
    public void concurrentOrderBeforeWithRemove() throws Exception {
        ContentSession s1 = repository.login(null, null);
        try {
            Root r1 = s1.getLatestRoot();
            Tree t1 = r1.getTree("/");
            t1.addChild("node1");
            t1.addChild("node2");
            t1.addChild("node3");
            t1.addChild("node4");
            r1.commit();
            t1 = r1.getTree("/");

            ContentSession s2 = repository.login(null, null);
            try {
                Root r2 = s2.getLatestRoot();
                Tree t2 = r2.getTree("/");

                t1.getChild("node2").orderBefore("node1");
                t1.getChild("node3").orderBefore(null);
                t1.getChild("node4").remove();
                r1.commit();
                t1 = r1.getTree("/");
                assertSequence(t1.getChildren(), "node2", "node1", "node3");

                t2.getChild("node3").orderBefore("node1");
                r2.commit();
                t2 = r2.getTree("/");
                // other session wins
                assertSequence(t2.getChildren(), "node2", "node1", "node3");

                // try again on current root
                t2.getChild("node3").orderBefore("node1");
                r2.commit();
                t2 = r2.getTree("/");
                assertSequence(t2.getChildren(), "node2", "node3", "node1");

            } finally {
                s2.close();
            }
        } finally {
            s1.close();
        }
    }

    @Test
    public void concurrentOrderBeforeRemoved() throws Exception {
        ContentSession s1 = repository.login(null, null);
        try {
            Root r1 = s1.getLatestRoot();
            Tree t1 = r1.getTree("/");
            t1.addChild("node1");
            t1.addChild("node2");
            t1.addChild("node3");
            r1.commit();
            t1 = r1.getTree("/");

            ContentSession s2 = repository.login(null, null);
            try {
                Root r2 = s2.getLatestRoot();
                Tree t2 = r2.getTree("/");

                t1.getChild("node2").orderBefore("node1");
                t1.getChild("node3").remove();
                r1.commit();
                t1 = r1.getTree("/");
                assertSequence(t1.getChildren(), "node2", "node1");

                t2.getChild("node3").orderBefore("node1");
                r2.commit();
                t2 = r2.getTree("/");
                assertSequence(t2.getChildren(), "node2", "node1");

            } finally {
                s2.close();
            }
        } finally {
            s1.close();
        }
    }

    @Test
    public void concurrentOrderBeforeTargetRemoved() throws Exception {
        ContentSession s1 = repository.login(null, null);
        try {
            Root r1 = s1.getLatestRoot();
            Tree t1 = r1.getTree("/");
            t1.addChild("node1").orderBefore(null);
            t1.addChild("node2");
            t1.addChild("node3");
            t1.addChild("node4");
            r1.commit();
            t1 = r1.getTree("/");

            ContentSession s2 = repository.login(null, null);
            try {
                Root r2 = s2.getLatestRoot();
                Tree t2 = r2.getTree("/");

                t1.getChild("node2").orderBefore("node1");
                t1.getChild("node3").remove();
                r1.commit();
                t1 = r1.getTree("/");
                assertSequence(t1.getChildren(), "node2", "node1", "node4");

                t2.getChild("node4").orderBefore("node3");
                r2.commit();
                t2 = r2.getTree("/");
                assertSequence(t2.getChildren(), "node2", "node1", "node4");

            } finally {
                s2.close();
            }
        } finally {
            s1.close();
        }
    }

    @Test
    public void concurrentAddChildOrderable() throws Exception {
        ContentSession s1 = repository.login(null, null);
        try {
            Root r1 = s1.getLatestRoot();
            Tree t1 = r1.getTree("/");
            t1.addChild("node1").orderBefore(null);
            t1.addChild("node2");
            r1.commit();
            ContentSession s2 = repository.login(null, null);
            try {
                Root r2 = s2.getLatestRoot();
                Tree t2 = r2.getTree("/");

                t1 = r1.getTree("/");
                // node3 from s1
                t1.addChild("node3");
                r1.commit();

                // node4 from s2
                t2.addChild("node4");
                r2.commit();

                t1 = s1.getLatestRoot().getTree("/");
                assertSequence(
                        t1.getChildren(), "node1", "node2", "node3", "node4");
            } finally {
                s2.close();
            }
        } finally {
            s1.close();
        }

    }

    @Test
    public void concurrentAddChild() throws Exception {
        ContentSession s1 = repository.login(null, null);
        try {
            Root r1 = s1.getLatestRoot();
            Tree t1 = r1.getTree("/");
            t1.addChild("node1");
            t1.addChild("node2");
            t1.addChild("node3");
            r1.commit();
            ContentSession s2 = repository.login(null, null);
            try {
                Root r2 = s2.getLatestRoot();
                Tree t2 = r2.getTree("/");

                t1 = r1.getTree("/");
                // node4 from s1
                t1.addChild("node4");
                r1.commit();

                // node5 from s2
                t2.addChild("node5");
                r2.commit();

                r1 = s1.getLatestRoot();
                t1 = r1.getTree("/");
                Set<String> names = Sets.newHashSet();
                for (Tree t : t1.getChildren()) {
                    names.add(t.getName());
                }
                assertEquals(Sets.newHashSet("node1", "node2", "node3", "node4", "node5"), names);
            } finally {
                s2.close();
            }
        } finally {
            s1.close();
        }
    }
}
