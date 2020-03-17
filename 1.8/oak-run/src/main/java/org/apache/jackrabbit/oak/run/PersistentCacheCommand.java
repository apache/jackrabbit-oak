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

import static java.util.Arrays.asList;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jackrabbit.oak.run.commons.Command;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.type.StringDataType;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class PersistentCacheCommand implements Command {
    public final static String PERSISTENTCACHE = "persistentcache";

    @SuppressWarnings("unchecked")
    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<String> pathSpec = parser.accepts("path", 
                "only list entries starting with this path prefix").
                withOptionalArg().defaultsTo("/");
        OptionSpec<String> revisionSpec = parser.accepts("revision", 
                "only list revisions that start with this prefix").
                withRequiredArg().defaultsTo("");
        OptionSpec<String> mapSpec = parser.accepts("map", 
                "only print contents of this map").
                withRequiredArg().defaultsTo("");
        OptionSpec<Void> valuesSpec = parser.accepts("values", 
                "print values, not just keys and value lengths");
        OptionSpec<Void> rawSpec = parser.accepts("raw", 
                "print raw data (tab separated map name, key, length, value)");
        OptionSpec<String> outSpec = parser.accepts("out", 
                "print to this file instead of stdout").
                withRequiredArg().defaultsTo("");
        OptionSpec<?> helpSpec = parser.acceptsAll(
                asList("h", "?", "help"), "show help").forHelp();
        OptionSet options = parser.parse(args);
        parser.nonOptions(
                "persistent cache file (required)").ofType(File.class);
        if (options.has(helpSpec)
                || options.nonOptionArguments().isEmpty()) {
            System.out.println("Mode: " + PERSISTENTCACHE);
            System.out.println("Map names and statistic are listed if just the file name is specified.");
            System.out.println("To list all keys, just specify '/' and the file name.");
            System.out.println("To dump multiples files in one go, add multiple file names.");
                System.out.println("Files are accessed in read-only mode; " + 
                    "to analyze a running system you need to copy the cache file first.");
            System.out.println("Output format is CSV (',' replaced with '#')");
            System.out.println("To import in H2, use: " + 
                    "create table cache as select * from csvread('cache.csv', null, 'fieldDelimiter=')");
            System.out.println();
            parser.printHelpOn(System.out);
            return;
        }
        String path = pathSpec.value(options);
        String revision = revisionSpec.value(options);
        String map = mapSpec.value(options);
        boolean values = options.has(valuesSpec);
        boolean raw = options.has(rawSpec);
        String out = outSpec.value(options);
        PrintWriter write = new PrintWriter(System.out);
        if (out.length() > 0) {
            write = new PrintWriter(new BufferedWriter(new FileWriter(out)));
        }
        for (String fileName : ((List<String>) options.nonOptionArguments())) {
            dump(write, path, revision, map, fileName, values, raw);
        }      
        write.flush();
    }
    
    static void dump(PrintWriter write, String path, String revision, 
            String map, String fileName, boolean values, boolean raw) {
         MVStore s = new MVStore.Builder().readOnly().
                 fileName(fileName).open();
         Map<String, String> meta = s.getMetaMap();
         boolean statsOnly = "".equalsIgnoreCase(map) && 
                 "".equals(revision) && 
                 "".equals(path);
         if (!statsOnly) {
             if (raw) {
                 write.println("map" + "\t" + "key" + "\t" + "length" + "\t" + "value");
             } else if (values) {
                 write.println("map,path,revision,p2,length,value");
             } else {
                 write.println("map,path,revision,p2,length");
             }
         }
         for (String n : meta.keySet()) {
             if (n.startsWith("name.")) {
                 String mapName = n.substring(5, n.length());
                 if (map.length() > 0 && !map.equalsIgnoreCase(mapName)) {
                     continue;
                 }
                 MVMap.Builder<String, String> b = 
                         new MVMap.Builder<String, String>().
                         keyType(StringDataType.INSTANCE).valueType(
                                 StringDataType.INSTANCE);
                 MVMap<String, String> m = s.openMap(mapName, b);
                 if (statsOnly) {
                     statistics(write, m);
                 } else if (raw) {
                     dumpRaw(write, m);
                 } else {
                     dump(write, m, path, revision, values);
                 }
             }
         }
         s.close();        
    }

    static void statistics(PrintWriter write, MVMap<String, String> m) {
        write.println("map: " + m.getName().toLowerCase());
        write.println("entryCount: " + m.sizeAsLong());
        long keyLen = 0, valueLen = 0;
        for (Entry<String, String> e : m.entrySet()) {
            String k = e.getKey();
            String v = e.getValue();
            keyLen += k.length();
            valueLen += v.length();
        }
        write.println("keyLen: " + keyLen);
        write.println("valueLen: " + valueLen);
        write.println();
    }

    static void dumpRaw(PrintWriter write, MVMap<String, String> m) {
        String mapName = m.getName().toLowerCase();
        // map key value length
        for (Entry<String, String> e : m.entrySet()) {
            String key = e.getKey();
            String value = e.getValue();
            write.println(mapName + "\t" + key + "\t" + value.length() + "\t" + value);
        }
    }

    static void dump(PrintWriter write, MVMap<String, String> m, String path,
            String revision, boolean values) {
        String mapName = m.getName().toLowerCase();
        // map,path,revision,p2,value,length
        for (Entry<String, String> e : m.entrySet()) {
            String key = e.getKey();
            int slash = key.indexOf('/');
            String r2 = "";
            if (!key.startsWith("/") && slash > 0) {
                r2 = key.substring(0, slash).replace(',', '#');
                key = key.substring(slash);
            }
            if (!"/".equals(path) && !key.startsWith(path)) {
                continue;
            }
            int lastAt = key.lastIndexOf('@');
            String rev = "";
            if (r2.endsWith(":p")) {
                // prev_document, for example 0:p/r155a16928cd-0-1/0
                // format: ..p<path>r<revision>/<number>
                // we set r2 to <number>
                rev = key.substring(key.lastIndexOf('r'));
                key = key.substring(0, key.length() - rev.length());
                r2 = rev.substring(rev.lastIndexOf('/') + 1);
                rev = rev.substring(0, rev.lastIndexOf('/'));
            }
            if (lastAt > 0) {
                rev = key.substring(lastAt + 1).replace(',', '#');
                key = key.substring(0, lastAt);
            }
            if (!"".equals(revision) && !rev.startsWith(revision)) {
                continue;
            }
            String v = e.getValue();
            if (values) {
                v = v.length() + "," + v.replace(',', '#');
            } else {
                v = "" + v.length();
            }
            key = key.replace(',', '#');
            write.println(mapName + "," + key + "," + rev + "," + r2 + "," + v);
        }
    }    

}
