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

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfoDocument;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBJSONSupport;
import org.apache.jackrabbit.oak.run.commons.Command;

import com.google.common.io.Closer;

import joptsimple.OptionSpec;

class ClusterNodesCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        Closer closer = Closer.create();
        try {
            String h = "clusternodes mongodb://host:port/database|jdbc:...";
            ClusterNodesOptions options = new ClusterNodesOptions(h).parse(args);
            if (options.isHelp()) {
                options.printHelpOn(System.out);
                System.exit(0);
            }

            DocumentNodeStoreBuilder<?> builder = Utils.createDocumentMKBuilder(options, closer);

            if (builder == null) {
                System.err
                        .println("Clusternodes command only available for DocumentNodeStore backed by MongoDB or RDB persistence");
                System.exit(1);
            }

            builder.setReadOnlyMode();
            DocumentStore ds = builder.getDocumentStore();

            try {
                List<ClusterNodeInfoDocument> all = new ArrayList<>(ClusterNodeInfoDocument.all(ds));
                Collections.sort(all, new Comparator<ClusterNodeInfoDocument>() {
                    @Override
                    public int compare(ClusterNodeInfoDocument one, ClusterNodeInfoDocument two) {
                        return Integer.compare(one.getClusterId(), two.getClusterId());
                    }
                });
                if (options.isRaw()) {
                    printRaw(all);
                } else {
                    print(all, options.isVerbose());
                }
            } catch (Throwable e) {
                e.printStackTrace(System.err);
            }
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    private static void print(List<ClusterNodeInfoDocument> docs, boolean verbose) {

        String sId = "Id";
        String sState = "State";
        String sStarted = "Started";
        String sLeaseEnd = "LeaseEnd";
        String sRecoveryBy = "RecoveryBy";
        String sLeft = "Left";
        String sLastRootRev = "LastRootRev";
        String sOakVersion = "OakVersion";

        long now = System.currentTimeMillis();

        List<String> header = new ArrayList<>();
        header.addAll(Arrays.asList(new String[] { sId, sState, sStarted, sLeaseEnd, sLeft, sRecoveryBy }));
        if (verbose) {
            header.add(sLastRootRev);
            header.add(sOakVersion);
        }

        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));

        List<Map<String, String>> body = new ArrayList<>();

        for (ClusterNodeInfoDocument c : docs) {

            long start = c.getStartTime();
            long leaseEnd;
            long left;
            try {
                leaseEnd = c.getLeaseEndTime();
                left = (leaseEnd - now) / 1000;
            } catch (Exception ex) {
                leaseEnd = 0;
                left = Long.MIN_VALUE;
            }

            Map<String, String> e = new HashMap<>();
            e.put(sId, Integer.toString(c.getClusterId()));
            e.put(sState, c.isActive() ? "ACTIVE" : "INACTIVE");
            e.put(sStarted, start <= 0 ? "-" : df.format(new Date(start)));
            e.put(sLeaseEnd, leaseEnd == 0 ? "-" : df.format(new Date(leaseEnd)));
            e.put(sLeft, (left < -999) ? "-" : (Long.toString(left) + "s"));
            e.put(sRecoveryBy,
                    c.getRecoveryBy() == null ? (c.isRecoveryNeeded(now) ? "!" : "-") : Long.toString(c.getRecoveryBy()));

            if (verbose) {
                e.put(sLastRootRev, c.getLastWrittenRootRev());
                Object oakVersion = c.get("oakVersion");
                e.put(sOakVersion,  oakVersion == null ? "-" : oakVersion.toString());
            }
            body.add(e);
        }
        list(System.out, header, body);
    }

    /**
     * A generic method to print a table, choosing column widths automatically
     * based both on column title and values.
     * 
     * @param out
     *            output target
     * @param header
     *            list of column titles
     * @param body
     *            list of rows, where each row is a map from column title to
     *            value
     */
    private static void list(PrintStream out, List<String> header, List<Map<String, String>> body) {
        // find column widths
        Map<String, Integer> widths = new HashMap<>();
        for (String h : header) {
            widths.put(h, h.length());
        }
        for (Map<String, String> m : body) {
            for (Map.Entry<String, String> e : m.entrySet()) {
                int current = widths.get(e.getKey());
                int thisone = e.getValue().length();
                widths.put(e.getKey(), Math.max(current, thisone));
            }
        }
        StringBuilder sformat = new StringBuilder();
        for (String h : header) {
            if (sformat.length() != 0) {
                sformat.append(' ');
            }
            sformat.append("%" + widths.get(h) + "s");
        }
        String format = sformat.toString();

        out.println(String.format(format, header.toArray()));
        for (Map<String, String> m : body) {
            List<String> l = new ArrayList<>();
            for (String h : header) {
                l.add(m.get(h));
            }
            out.println(String.format(format, l.toArray()));
        }
    }

    private static void printRaw(Iterable<ClusterNodeInfoDocument> docs) {
        Map<Object, Object> rawEntries = new HashMap<>();
        for (ClusterNodeInfoDocument c : docs) {
            Map<Object, Object> entries = new HashMap<>();
            for (String k : c.keySet()) {
                entries.put(k, c.get(k));
            }
            rawEntries.put(Integer.toString(c.getClusterId()), entries);
        }
        StringBuilder sb = new StringBuilder();
        RDBJSONSupport.appendJsonMap(sb, rawEntries);
        System.out.println(sb);
    }

    private static final class ClusterNodesOptions extends Utils.NodeStoreOptions {

        final OptionSpec<Void> raw;
        final OptionSpec<Void> verbose;

        ClusterNodesOptions(String usage) {
            super(usage);
            raw = parser.accepts("raw", "List raw entries in JSON format");
            verbose = parser.accepts("verbose", "Be more verbose");
        }

        @Override
        public ClusterNodesOptions parse(String[] args) {
            super.parse(args);
            return this;
        }

        boolean isRaw() {
            return options.has(raw);
        }

        boolean isVerbose() {
            return options.has(verbose);
        }

        boolean isHelp() {
            return options.has(help);
        }
    }
}
