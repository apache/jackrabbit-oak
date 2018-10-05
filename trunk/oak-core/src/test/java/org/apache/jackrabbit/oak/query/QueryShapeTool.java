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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.TreeSet;

/**
 * A tool that converts a list of queries to parameterized queries. This for
 * example allows to extract unique queries.
 */
public class QueryShapeTool {

    public static void main(String... args) throws IOException {
        process(new File(args[0]));
    }

    public static void process(File file) throws IOException {
        processFile(file);
    }

    private static void processFile(File file) throws IOException {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                processFile(f);
            }
            return;
        }
        System.out.println("File " + file);
        LineNumberReader r = new LineNumberReader(new BufferedReader(
                new FileReader(file)));
        try {
            process(r);
        } finally {
            r.close();
        }
        System.out.println();
    }

    public static void process(LineNumberReader reader) throws IOException {
        TreeSet<String> sortedUnique = new  TreeSet<String>();
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            sortedUnique.add(shape(line));
        }
        for (String s : sortedUnique) {
            System.out.println(s);
        }
    }
    
    public static String shape(String query) {
        String result = query;
        // replace double quoted string literals with "$s"
        result = result.replaceAll("\"[^\"]*\"", "\\$s");
        // replace single quoted string literals with "$s"
        result = result.replaceAll("'[^\']*\'", "\\$s");
        // replace repeated "$s" with a single one (due to escape characters in string literals)
        result = result.replaceAll("(\\$s)+", "\\$s");

        // xpath: replace "//" with "/ /" so we can more easily stop there
        result = result.replaceAll("//", "/ /");
        // xpath: replace "/element(" with "/ element" for the same reason
        result = result.replaceAll("/element\\(", "/ element\\(");
        // xpath: replace "/text(" with "/ text" for the same reason
        result = result.replaceAll("/text\\(", "/ text\\(");
        // xpath: replace a path at the beginning of the query with $path
        result = result.replaceAll("/jcr:root(/([^ /]*))*[ /]", "/jcr:root/\\$path/");
        return result;
    }

}