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
package org.apache.jackrabbit.mk;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.json.simple.parser.ParseException;

/**
 * A simple hello world app.
 */
public class HelloWorld {

    public static void main(String... args) throws ParseException {
        test("fs:{homeDir};clean");
        test("simple:");
        test("simple:fs:target/temp;clean");
    }

    private static void test(String url) throws ParseException {
        MicroKernel mk = MicroKernelFactory.getInstance(url);
        System.out.println(url);
        String head = mk.getHeadRevision();
        head = mk.commit("/", "+ \"hello\" : {}", head, null);
        String move = "> \"hello\": \"world\" ";
        String set = "^ \"world/x\": 1 ";
        try {
            head = mk.commit("/", move + set, head, null);
            System.out.println("move & set worked");
        } catch (Exception e) {
            System.out.println("move & set didn't work:");
            e.printStackTrace(System.out);
            head = mk.commit("/", move, head, null);
            head = mk.commit("/", set, head, null);
        }
        System.out.println();
        mk.dispose();
    }

}
