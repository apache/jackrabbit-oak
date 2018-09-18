<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
  -->

# Repository Construction

Oak comes with a simple and flexible mechanism for constructing content repositories
for use in embedded deployments and test cases. This article describes this
mechanism. Deployments in managed environments like OSGi should use the native
construction/configuration mechanism of the environment.

First, we construct a Repository instance.
Both the `Oak` and the `Jcr` classes support `with()` methods, 
so you can easily extend the repository with custom functionality if you like.
To construct an in-memory repository, use:

        Repository repo = new Jcr(new Oak()).createRepository();

To use a tar file based Segment NodeStore backend, use:

        FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("repository")).build();
        SegmentNodeStore ns = SegmentNodeStoreBuilders.builder(fs).build();
        Repository repo = new Jcr(new Oak(ns)).createRepository();

To use a MongoDB backend, use:

        DB db = new MongoClient("127.0.0.1", 27017).getDB("test2");
        DocumentNodeStore ns = new DocumentMK.Builder().
                setMongoDB(db).getNodeStore();
        Repository repo = new Jcr(new Oak(ns)).createRepository();

To login to the repository and do some work (using
the default username/password combination), use:

        Session session = repo.login(
                new SimpleCredentials("admin", "admin".toCharArray()));
        Node root = session.getRootNode();
        if (root.hasNode("hello")) {
            Node hello = root.getNode("hello");
            long count = hello.getProperty("count").getLong();
            hello.setProperty("count", count + 1);
            System.out.println("found the hello node, count = " + count);
        } else {
            System.out.println("creating the hello node");
            root.addNode("hello").setProperty("count", 1);
        }
        session.save();
        
To logout and close the backend store, use:
        
        session.logout();
        // depending on NodeStore implementation either:
        // close FileStore
        fs.close();
        // or close DocumentNodeStore
        ns.dispose();
