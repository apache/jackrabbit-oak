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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.DocumentQueue;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import com.google.common.io.Closer;
import com.google.common.util.concurrent.MoreExecutors;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Scans and lists all references to nt:frozenNode and returns an exit code of 1 if any are found (0 otherwise).
 * <p>
 * This variant does a *very expensive repository scan* for all properties formatted as uuid
 * ( LIKE \"________-____-____-____-____________\" )
 * and checking if any reference points to an nt:frozenNode (under /jcr:system/jcr:versionStorage
 * at depth &gt; 7).
 * <p>
 * Note that any property with uuid that cannot be resolved will *not be reported*, as that
 * is a legitimate use case of uuid property use. Only uuids that resolve will be analysed.
 * <p>
 * Example:
 * <pre>
 * java -mx4g -jar oak-run-*.jar frozennoderefsbyscanning mongodb://localhost/&lt;dbname&gt; -user=admin -password=admin
 * </pre>
 */
public class FrozenNodeRefsByScanningCommand implements Command {

    static {
        // disable any query limits as our query is going to be a full scan, and we are aware of it
        System.setProperty("oak.queryLimitReads", String.valueOf(Long.MAX_VALUE));

        //		// disable the WARN of the TraversingCursor
        //		LoggerContext c = (LoggerContext) LoggerFactory.getILoggerFactory();
        //		Logger logger = c.getLogger("org.apache.jackrabbit.oak.plugins.index.Cursors$TraversingCursor");
        //		logger.setLevel(Level.ERROR);
    }

    public static final String NAME = "frozennoderefsbyscanning";

    private final String summary = "Scans repository and lists all references to nt:frozenNode";

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();

        Options opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(summary);
        opts.setConnectionString(CommonOptions.DEFAULT_CONNECTION_STRING);

        OptionSpec<String> userOption = parser.accepts("user", "User name").withOptionalArg().defaultsTo("admin");
        OptionSpec<String> passwordOption = parser.accepts("password", "Password").withOptionalArg().defaultsTo("admin");

        OptionSet options = opts.parseAndConfigure(parser, args);

        System.out.println("Opening nodestore (readOnly=true)...");
        // explicitly set readOnly mode (overwriting any possibly set -read-write= option)
        NodeStoreFixture nodeStoreFixture = NodeStoreFixtureProvider.create(opts, true);
        System.out.println("Nodestore opened.");

        int count = uuidscan(userOption, passwordOption, options, nodeStoreFixture);
        if (count > 0) {
            System.err.println("FAILURE: " + count + " Reference(s) (in any uuid formatted property value) to nt:frozenNode found.");
            System.exit(1);
        } else {
            System.out.println("SUCCESS: No references (in any uuid formatted property value) to nt:frozenNode found.");
        }
    }

    /**
     * Scans the repository (via an expensive traversing query, ouch, hence it needs username/password)
     * and returns the number of references found. For this, any value formatted like a UUID is considered,
     * then verified if it points to an nt:frozenNode.
     */
    private int uuidscan(OptionSpec<String> userOption, OptionSpec<String> passwordOption,
            OptionSet options, NodeStoreFixture nodeStoreFixture) throws IOException {
        NodeStore nodeStore = nodeStoreFixture.getStore();
        String user = userOption.value(options);
        String password = passwordOption.value(options);

        Closer closer = Utils.createCloserWithShutdownHook();
        closer.register(nodeStoreFixture);

        int count = 0;

        try {

            System.out.println("Logging in...");
            Session session = openSession(nodeStore, "crx.default", user, password);

            System.out.println("Logged in, querying...");
            QueryManager qm = session.getWorkspace().getQueryManager();

            // query for only getting 'Reference' and 'WeakReference' would be :
            // SELECT * FROM [nt:base] AS p WHERE PROPERTY(*, 'Reference') IS NOT NULL OR PROPERTY(*, 'WeakReference') IS NOT NULL
            Query q = qm.createQuery("SELECT * FROM [nt:base] AS p WHERE PROPERTY(*, '*') LIKE \"________-____-____-____-____________\"", "JCR-SQL2");
            QueryResult qr = q.execute();
            NodeIterator it = qr.getNodes();

            while (it.hasNext()) {
                Node n = it.nextNode();
                PropertyIterator pit = n.getProperties();
                while (pit.hasNext()) {
                    Property p = pit.nextProperty();
                    if ("jcr:uuid".equals(p.getName())) {
                        // jcr:uuid should be skipped as that's the identifier of a node, not a reference
                        continue;
                    }
                    if (!p.isMultiple()) {
                        String propValue = p.getValue().getString();
                        if (propValue.matches("........-....-....-....-............")) {
                            if (verify(session, n, p, propValue)) {
                                count++;
                            }
                        }
                    } else {
                        for (Value v : p.getValues()) {
                            String propValue = v.getString();
                            if (propValue.matches("........-....-....-....-............")) {
                                if (verify(session, n, p, propValue)) {
                                    count++;
                                }
                            }
                        }
                    }
                }
            }

            System.out.println("logout...");
            session.logout();
            System.out.println("done.");

            return count;
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    private boolean verify(Session session, Node n, Property p, String propValue) throws RepositoryException {
        try {
            Node node = session.getNodeByIdentifier(propValue);
            String path = node.getPath();
            boolean candidate = FrozenNodeRef.isFrozenNodeReferenceCandidate(path);
            if (!candidate) {
                return false;
            }
            Property primaryType = node.getProperty("jcr:primaryType");
            String primaryTypeValue = primaryType.getString();
            boolean isNtFrozenNode = "nt:frozenNode".equals(primaryTypeValue);
            if (!isNtFrozenNode) {
                // this is where we ultimately have to continue out in any case - as only an nt:frozenNode
                // is what we're interested in.
                return false;
            }

            String uuid = propValue;
            String referrerPath = n.getPath();
            String referrerProperty = p.getName();
            FrozenNodeRef ref = new FrozenNodeRef(referrerPath, referrerProperty, PropertyType.nameFromValue(p.getType()), uuid, path);

            System.out.println(FrozenNodeRef.REFERENCE_TO_NT_FROZEN_NODE_FOUND_PREFIX + ref.toInfoString());

            return true;
        } catch (ItemNotFoundException notFound) {
            // then ignore
            return false;
        }
    }

    // from JsonIndexCommand, slightly modified to be able to set the workspace name
    public static Session openSession(NodeStore nodeStore, String workspaceName, String user, String password) throws RepositoryException {
        if (nodeStore == null) {
            return null;
        }
        StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;
        Oak oak = new Oak(nodeStore).with(ManagementFactory.getPlatformMBeanServer());
        oak.getWhiteboard().register(StatisticsProvider.class, statisticsProvider, Collections.emptyMap());
        LuceneIndexProvider provider = /*JsonIndexCommand.*/createLuceneIndexProvider();
        oak.with((QueryIndexProvider) provider).with((Observer) provider).with(/*JsonIndexCommand.*/createLuceneIndexEditorProvider());
        Jcr jcr = new Jcr(oak);
        jcr.with(workspaceName);
        Repository repository = jcr.createRepository();
        return repository.login(new SimpleCredentials(user, password.toCharArray()));
    }

    // from JsonIndexCommand, unmodified
    private static LuceneIndexEditorProvider createLuceneIndexEditorProvider() {
        LuceneIndexEditorProvider ep = new LuceneIndexEditorProvider();
        ScheduledExecutorService executorService = MoreExecutors
                .getExitingScheduledExecutorService((ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(5));
        StatisticsProvider statsProvider = StatisticsProvider.NOOP;
        int queueSize = Integer.getInteger("queueSize", 1000);
        long queueTimeout = Long.getLong("queueTimeoutMillis", 100);
        IndexTracker tracker = new IndexTracker();
        DocumentQueue queue = new DocumentQueue(queueSize, queueTimeout, tracker, executorService, statsProvider);
        ep.setIndexingQueue(queue);
        return ep;
    }

    // from JsonIndexCommand, unmodified
    private static LuceneIndexProvider createLuceneIndexProvider() {
        return new LuceneIndexProvider();
    }
}
