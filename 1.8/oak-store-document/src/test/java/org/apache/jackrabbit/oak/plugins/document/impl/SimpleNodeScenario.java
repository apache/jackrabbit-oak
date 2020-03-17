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
package org.apache.jackrabbit.oak.plugins.document.impl;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;

/**
 * A class that can perform simple operations.
 */
public class SimpleNodeScenario {

    private final DocumentMK mk;

    public SimpleNodeScenario(DocumentMK mk) {
        this.mk = mk;
    }

    public String create() throws Exception {
        return mk.commit("/",
                "+\"a\" : { \"int\" : 1 , \"b\" : { \"string\" : \"foo\" } , \"c\" : { \"bool\" : true } }",
                null,
                "Simple node scenario with nodes /, /a, /a/b, /a/c");
    }

    public String addChildrenToA(int count) throws Exception {
        String revisionId = null;
        for (int i = 1; i <= count; i++) {
            revisionId = mk.commit("/a", "+\"child" + i + "\" : {}", null, "Add child" + i);
        }
        return revisionId;
    }

    public String deleteA() throws Exception {
        return mk.commit("/", "-\"a\"", null, "Commit with deleted /a");
    }

    public String deleteB() throws Exception {
        return mk.commit("/a", "-\"b\"", null, "Commit with deleted /a/b");
    }

}
