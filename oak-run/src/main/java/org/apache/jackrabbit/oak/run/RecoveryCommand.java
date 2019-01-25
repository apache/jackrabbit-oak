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

package org.apache.jackrabbit.oak.run;

import java.io.Closeable;
import java.util.Arrays;

import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.LastRevRecoveryAgent;
import org.apache.jackrabbit.oak.plugins.document.MissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoMissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBMissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.util.MapDBMapFactory;
import org.apache.jackrabbit.oak.plugins.document.util.MapFactory;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.io.Closer;

class RecoveryCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        MapFactory.setInstance(new MapDBMapFactory());
        Closer closer = Closer.create();
        String h = "recovery mongodb://host:port/database|jdbc:... { dryRun }";
        try {
            NodeStore store = Utils.bootstrapNodeStore(args, closer, h);

            if (!(store instanceof DocumentNodeStore)) {
                System.err.println("Recovery only available for DocumentNodeStore");
                System.exit(1);
            }

            DocumentNodeStore dns = (DocumentNodeStore) store;
            DocumentStore ds = dns.getDocumentStore();
            LastRevRecoveryAgent agent = null;
            MissingLastRevSeeker seeker = null;

            if (ds instanceof MongoDocumentStore) {
                MongoDocumentStore docStore = (MongoDocumentStore) ds;
                agent = new LastRevRecoveryAgent(docStore, dns);
                seeker = new MongoMissingLastRevSeeker(docStore, dns.getClock());
            } else if (ds instanceof RDBDocumentStore) {
                RDBDocumentStore docStore = (RDBDocumentStore) ds;
                agent = new LastRevRecoveryAgent(docStore, dns);
                seeker = new RDBMissingLastRevSeeker(docStore, dns.getClock());
            }

            if (agent == null || seeker == null) {
                System.err.println("Recovery only available for MongoDocumentStore and RDBDocumentStore (this: "
                        + ds.getClass().getName() + ")");
                System.exit(1);
            }

            Iterable<NodeDocument> docs = seeker.getCandidates(0);
            if (docs instanceof Closeable) {
                closer.register((Closeable) docs);
            }
            boolean dryRun = Arrays.asList(args).contains("dryRun");
            agent.recover(docs, dns.getClusterId(), dryRun);
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }
}
