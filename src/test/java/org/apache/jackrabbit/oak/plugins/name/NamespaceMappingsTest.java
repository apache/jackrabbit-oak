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

import static org.junit.Assert.assertEquals;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.junit.Test;

import javax.jcr.GuestCredentials;

public class NamespaceMappingsTest {

    @Test
    public void testMappings() throws Exception {
        ContentRepository repository = new ContentRepositoryImpl();
        ContentSession session = repository.login(new GuestCredentials(), "default");
        NamespaceMappings r = new NamespaceMappings(session);

        r.registerNamespace("p", "n");
        assertEquals(r.getURI("p"), "n");
        assertEquals(r.getPrefix("n"), "p");

        r.registerNamespace("p", "n2");
        assertEquals(r.getURI("p"), "n2");
        assertEquals(r.getPrefix("n2"), "p");

        r.registerNamespace("p2", "n");
        assertEquals(r.getURI("p2"), "n");
        assertEquals(r.getPrefix("n"), "p2");
        assertEquals(r.getURI("p"), "n2");
        assertEquals(r.getPrefix("n2"), "p");

        r.registerNamespace("p2", "n2");
        assertEquals(r.getURI("p2"), "n2");
        assertEquals(r.getPrefix("n2"), "p2");

        r.registerNamespace("p", "n");
        assertEquals(r.getURI("p2"), "n2");
        assertEquals(r.getPrefix("n2"), "p2");
        assertEquals(r.getURI("p"), "n");
        assertEquals(r.getPrefix("n"), "p");
    }

}
