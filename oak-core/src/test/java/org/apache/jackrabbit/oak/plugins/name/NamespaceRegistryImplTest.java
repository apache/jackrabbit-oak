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
package org.apache.jackrabbit.oak.plugins.name;

import javax.jcr.NamespaceRegistry;

import org.apache.jackrabbit.oak.AbstractOakTest;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NamespaceRegistryImplTest extends AbstractOakTest {

    @Override
    protected ContentRepository createRepository() {
        return createEmptyRepository();
    }

    @Test
    public void testMappings() throws Exception {
        final ContentSession session = createAdminSession();
        NamespaceRegistry r = new NamespaceRegistryImpl() {
            @Override
            protected Tree getReadTree() {
                return session.getLatestRoot().getTree("/");
            }
            @Override
            protected Root getWriteRoot() {
                return session.getLatestRoot();
            }
        };

        assertEquals("", r.getURI(""));
        assertEquals("http://www.jcp.org/jcr/1.0", r.getURI("jcr"));
        assertEquals("http://www.jcp.org/jcr/nt/1.0", r.getURI("nt"));
        assertEquals("http://www.jcp.org/jcr/mix/1.0", r.getURI("mix"));
        assertEquals("http://www.w3.org/XML/1998/namespace", r.getURI("xml"));

        assertEquals("", r.getPrefix(""));
        assertEquals("jcr", r.getPrefix("http://www.jcp.org/jcr/1.0"));
        assertEquals("nt", r.getPrefix("http://www.jcp.org/jcr/nt/1.0"));
        assertEquals("mix", r.getPrefix("http://www.jcp.org/jcr/mix/1.0"));
        assertEquals("xml", r.getPrefix("http://www.w3.org/XML/1998/namespace"));

        r.registerNamespace("p", "n");
        assertEquals(r.getURI("p"), "n");
        assertEquals(r.getPrefix("n"), "p");

        r.registerNamespace("p2", "n2");
        assertEquals(r.getURI("p2"), "n2");
        assertEquals(r.getPrefix("n2"), "p2");

    }
}
