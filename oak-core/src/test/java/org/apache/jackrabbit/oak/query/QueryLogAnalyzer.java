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
package org.apache.jackrabbit.oak.query;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;
import java.util.TreeMap;

/**
 * A simple standalone tool to analyze a log file and list slow queries.
 */
public class QueryLogAnalyzer {
    
    public static void main(String... args) throws IOException {
        if (args.length == 0) {
            System.out.println("Usage: java ... <logFile> [<logFile2> ...]");
            return;
        }
        for (String arg : args) {
            analyze(arg);
        }
    }

    private static void analyze(String fileName) throws IOException {
        LineNumberReader r = new LineNumberReader(new FileReader(fileName));
        int totalLineCount = 0, traversedLineCount = 0;
        TreeMap<String, QueryStats> queries = new TreeMap<String, QueryStats>();
        while (true) {
            String line = r.readLine();
            if (line == null) {
                break;
            }
            totalLineCount++;
            String number = retrieve(line, "Traversed ", " nodes ");
            if (number == null) {
                continue;
            }
            traversedLineCount++;
            line = line.substring(line.indexOf("Traversed " + number + " nodes "));
            String index = retrieve(line, "nodes using index ", " with filter");
            String rawQuery = retrieve(line, "Filter(query=", null);
            if (rawQuery == null) {
                System.out.println("Unknown line: " + line);
            }
            String rawXpath = retrieve(rawQuery, "/* xpath: ", "*/, ");
            String query = filterParams(rawQuery);
            if (rawXpath != null) {
                rawQuery = rawXpath;
                query = filterParams(rawXpath);
            }
            String key = query;
            QueryStats q = queries.get(key);
            if (q == null) {
                q = new QueryStats();
                queries.put(key, q);
                q.query = query;
            }
            q.index = index;
            q.lineCount++;
            int nodes = Integer.parseInt(number);
            if (nodes > q.maxNodeCount) {
                q.longestQuery = rawQuery;
                q.maxNodeCount = nodes;
            }
            if (q.lastNodeCount == 0 || nodes < q.lastNodeCount) {
                // start
                q.runCount++;
                q.lastNodeCount = 0;
            } else {
                // continuation
            }
            q.nodeCount += nodes - q.lastNodeCount;
            q.lastNodeCount = nodes;
        }
        System.out.println("File: " + fileName);
        System.out.println("Lines: " + totalLineCount);
        System.out.println("Lines with 'Traversed': " + traversedLineCount);
        ArrayList<QueryStats> list = new ArrayList<QueryStats>();
        list.addAll(queries.values());
        Collections.sort(list);
        for (QueryStats q : list) {
            System.out.println();
            System.out.println("  Query: " + formatQuery(q.query));
            if (!q.query.equals(q.longestQuery)) {
                System.out.println("  Longest: " + formatQuery(q.longestQuery));
            }
            if (q.index != null) {
                System.out.println("  Index: " + q.index);
            }
            System.out.printf("  %,d nodes traversed; " + 
                    "ran %,d times, max %,d nodes, %,d lines\n",
                    q.nodeCount, 
                    q.runCount, q.maxNodeCount, q.lineCount                    
                    );
        }
        System.out.println();
        r.close();
    }
    
    private static String formatQuery(String q) {
        if (q.toLowerCase(Locale.ENGLISH).indexOf("select ") >= 0) {
            // (?i) means case insensitive
            q = q.replaceAll("(?i)from ", "\n        from ");
            q = q.replaceAll("(?i)where ", "\n        where ");
            q = q.replaceAll("(?i)and ", "\n        and ");
            q = q.replaceAll("(?i)or ", "\n        or ");
            q = q.replaceAll("(?i)order by ", "\n        order by ");
        } else {
            q = q.replaceAll("\\[", "\n        [");
            q = q.replaceAll("and ", "\n        and ");
            q = q.replaceAll("or ", "\n        or ");
            q = q.replaceAll("order by ", "\n        order by ");
        }
        return q;
    }
    
    private static String filterParams(String x) {
        // uuid
        x = x.replaceAll("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", 
                "<uuid>");
        // timestamp
        x = x.replaceAll("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]*[Z]", 
                "<timestamp>");
        // number
        x = x.replaceAll("[0-9]+", 
                "0");

        // timestamp
        x = x.replaceAll("<timestamp>", 
                "2000-01-01T00:00:00.000Z");
        // uuid
        x = x.replaceAll("<uuid>", 
                "12345678-1234-1234-123456789012");
        return x;
    }
    
    private static String retrieve(String s, String prefix, String suffix) {
        int start = s.indexOf(prefix);
        if (start < 0) {
            return null;
        }
        start += prefix.length();
        int end;
        if (suffix == null) {
            end = s.length();
        } else {
            end = s.indexOf(suffix, start);
            if (end < 0) {
                return null;
            }
        }
        return s.substring(start, end);
    }
    
    static class QueryStats implements Comparable<QueryStats> {
        String query;
        String longestQuery;
        String index;
        int lastNodeCount;
        int lineCount;
        int nodeCount;
        int runCount;
        int maxNodeCount;
        
        @Override
        public int compareTo(QueryStats o) {
            int comp = Long.signum(o.nodeCount - nodeCount);
            if (comp == 0) {
                comp = Long.signum(o.runCount - runCount);
            }
            if (comp == 0) {
                comp = o.query.compareTo(query);
            }
            return comp;
        }
    }

}
