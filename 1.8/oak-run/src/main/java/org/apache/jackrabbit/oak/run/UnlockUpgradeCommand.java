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
package org.apache.jackrabbit.oak.run;

import java.util.List;

import javax.sql.DataSource;

import com.mongodb.MongoClientURI;

import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static com.mongodb.MongoURI.MONGODB_PREFIX;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore.VERSION;

/**
 * Unlocks the DocumentStore for an upgrade to the current DocumentNodeStore
 * version.
 */
class UnlockUpgradeCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        // RDB specific options
        OptionSpec<String> rdbjdbcuser = parser.accepts("rdbjdbcuser", "RDB JDBC user").withOptionalArg().defaultsTo("");
        OptionSpec<String> rdbjdbcpasswd = parser.accepts("rdbjdbcpasswd", "RDB JDBC password").withOptionalArg().defaultsTo("");

        OptionSpec<String> nonOption = parser.nonOptions("unlockUpgrade {<jdbc-uri> | <mongodb-uri>}");
        OptionSpec help = parser.acceptsAll(asList("h", "?", "help"), "show help").forHelp();

        OptionSet options = parser.parse(args);
        List<String> nonOptions = nonOption.values(options);

        if (options.has(help)) {
            parser.printHelpOn(System.out);
            return;
        }

        if (nonOptions.isEmpty()) {
            parser.printHelpOn(System.err);
            return;
        }

        DocumentStore store = null;
        try {
            String uri = nonOptions.get(0);
            if (uri.startsWith(MONGODB_PREFIX)) {
                MongoClientURI clientURI = new MongoClientURI(uri);
                if (clientURI.getDatabase() == null) {
                    System.err.println("Database missing in MongoDB URI: " + clientURI.getURI());
                } else {
                    MongoConnection mongo = new MongoConnection(clientURI.getURI());
                    store = new MongoDocumentStore(mongo.getDB(), new MongoDocumentNodeStoreBuilder());
                }
            } else if (uri.startsWith("jdbc")) {
                DataSource ds = RDBDataSourceFactory.forJdbcUrl(uri,
                        rdbjdbcuser.value(options), rdbjdbcpasswd.value(options));
                store = new RDBDocumentStore(ds, new DocumentNodeStoreBuilder());
            } else {
                System.err.println("Unrecognized URI: " + uri);
            }

            if (store != null && VERSION.writeTo(store)) {
                System.out.println("Format version set to " + VERSION);
            }
        } catch (DocumentStoreException e) {
            System.err.println(e.getMessage());
        } finally {
            if (store != null) {
                store.dispose();
            }
        }
    }
}
