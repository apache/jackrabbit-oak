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
package org.apache.jackrabbit.oak.query.stats;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryRecorder {

    private static final Logger LOG = LoggerFactory.getLogger(QueryRecorder.class);
    private static final HashMap<String, Integer> RECORD_QUERIES_MAP = new HashMap<String, Integer>();
    private final static int QUERIES_MAX =  Integer.getInteger("oak.query.record", 5000);
    
    public static void main(String... args) throws IOException {
        // command line version: read from a file
        LineNumberReader reader = new LineNumberReader(
                new BufferedReader(new FileReader(args[0])));
        TreeSet<String> sorted = new TreeSet<String>();
        int lineCount = 0;
        while(true) {
            String line = reader.readLine();
            if(line == null) {
                break;
            }
            sorted.add(simplify(line));
            lineCount++;
        }
        reader.close();
        for(String s : sorted) {
            System.out.println(s);
        }
        System.out.println("sorted: " + sorted.size() + " original: " + lineCount);
    }
    
    public static void record(String query, boolean internal) {
        if (internal) {
            return;
        }
        if (!LOG.isDebugEnabled()) {
            return;
        }
        query = query.replace('\n', ' ');
        query = query.replace('\t', ' ');
        if (LOG.isTraceEnabled()) {
            LOG.trace("query:\t{}", query);
        }
        try {
            query = simplify(query);
        } catch (Exception e) {
            LOG.trace("failed to simplify {}", query, e);
        }
        record(query);
    }
    
    public static String simplify(String query) {
        query = query.replaceAll("'[^']*'", "'x'");
        query = query.replaceAll("ISDESCENDANTNODE\\(\\[/[^]]*\\]\\)", "ISDESCENDANTNODE('x')");
        int pathIndex = query.indexOf("/jcr:root/");
        if (pathIndex >= 0) {
            int start = pathIndex + "/jcr:root/".length();
            int end = getFirstOccurance(query, start,
                    " ", "/element(", "/text(", "/*", "/(", "/jcr:deref(");
            String path = query.substring(start, end);
            int first = path.indexOf('/');
            if (first > 0) {
                first = path.indexOf('/', first + 1);
                if (first > 0) {
                    path = path.substring(0, first + 1) + "...";
                }
            }
            String newQuery = query.substring(0, pathIndex) + "/jcr:root/" + path + query.substring(end, query.length());
            query = newQuery;
        }
        return query;
    }
    
    static int getFirstOccurance(String text, int start, String... strings) {
        int first = text.length();
        for(String s : strings) {
            int index = text.indexOf(s, start + 1);
            if (index > 0 && index < first) {
                first = index;
            }
        }
        return first;
    }
    
    private static void record(String query) {
        HashMap<String, Integer> map = RECORD_QUERIES_MAP;
        if (map.size() > QUERIES_MAX) {
            HashMap<String, Integer> old;
            synchronized (map) {
                old = new HashMap<>(map);
                map.clear();
            }
            for(Entry<String, Integer> e : old.entrySet()) {
                log(e.getKey(), e.getValue());
            }
        }
        Integer count;
        synchronized (map) {
            count = map.get(query);
            count = count == null ? 1 : count + 1;
            map.put(query, count);
        }
        if (count == 1 || count % 100 == 0) {
            log(query, count);
        }
    }

    private static void log(String query, int count) {
        LOG.debug("count:\t{}\tquery:\t{}", count, query);
    }

}
