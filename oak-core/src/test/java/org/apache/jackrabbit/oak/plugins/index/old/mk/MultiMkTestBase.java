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
package org.apache.jackrabbit.oak.plugins.index.old.mk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.index.old.mk.MicroKernelFactory;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeImpl;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeMap;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized.Parameters;

/**
 * The base class for tests that are run using multiple MicroKernel
 * implementations.
 */
public class MultiMkTestBase {

    private static final boolean PROFILE = false;

    public MicroKernel mk;
    protected String url;
    private Profiler prof;

    public MultiMkTestBase(String url) {
        this.url = url;
    }

    @Parameters
    public static Collection<Object[]> urls() {
            return Arrays.asList(new Object[][]{
                    {"simple:fs:target/temp"},
                    {"fs:{homeDir}/target"},
                    {"http-bridge:fs:{homeDir}/target"},
                    {"simple:"}
                    });
    }

    @Before
    public void setUp() throws Exception {
        mk = MicroKernelFactory.getInstance(url + ";clean");
        cleanRepository(mk);

        String root = mk.getNodes("/", mk.getHeadRevision(), 1, 0, -1, null);
        NodeImpl rootNode = NodeImpl.parse(root);
        if (rootNode.getPropertyCount() > 0) {
            System.out.println("Last mk not disposed: " + root);
        }
        if (rootNode.getChildNodeNames(Integer.MAX_VALUE).hasNext()) {
            System.out.println("Last mk not disposed: " + root);
        }
        if (PROFILE) {
            prof = new Profiler();
            prof.interval = 1;
            prof.depth = 32;
            prof.startCollecting();
        }
    }

    @After
    public void tearDown() throws InterruptedException {
        if (prof != null) {
            System.out.println(prof.getTop(5));
        }
        MicroKernelFactory.disposeInstance(mk);
    }

    protected void reconnect() {
        if (mk != null) {
            if (url.equals("simple:")) {
                return;
            }
            MicroKernelFactory.disposeInstance(mk);
        }
        mk = MicroKernelFactory.getInstance(url);
    }

    /**
     * Whether this is (directly or indirectly) the MemoryKernelImpl.
     *
     * @param mk the MicroKernel implementation
     * @return true if it is
     */
    public static boolean isSimpleKernel(MicroKernel mk) {
        return mk.nodeExists("/:info", mk.getHeadRevision());
    }

    private static void cleanRepository(MicroKernel mk) {
        String result = mk.getNodes("/", mk.getHeadRevision(), 0, 0, -1, null);
        List<String> names = new ArrayList<String>();
        List<String> properties = new ArrayList<String>();
        JsopReader t = new JsopTokenizer(result);
        t.read('{');
        if (!t.matches('}')) {
            do {
                String key = t.readString();
                t.read(':');
                if (t.matches('{')) {
                    names.add(key);
                    NodeImpl.parse(new NodeMap(), t, 0);
                } else {
                    if (!key.equals(":childNodeCount")) {
                        properties.add(key);
                    } else if (!key.equals(":hash")) {
                        properties.add(key);
                    }
                    t.readRawValue();
                }
            } while (t.matches(','));
            t.read('}');
        }
        if (!names.isEmpty()) {
            JsopBuilder buff = new JsopBuilder();
            for (String name : names) {
                buff.tag('-').value(name).newline();
            }
            mk.commit("/", buff.toString(), mk.getHeadRevision(), null);
        }
        if (!properties.isEmpty()) {
            JsopBuilder buff = new JsopBuilder();
            for (String property : properties) {
                buff.tag('^').key(property).value(null).newline();
            }
            mk.commit("/", buff.toString(), mk.getHeadRevision(), null);
        }
    }

}
