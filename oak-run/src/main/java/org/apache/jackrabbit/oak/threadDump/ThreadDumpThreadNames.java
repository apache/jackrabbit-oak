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
package org.apache.jackrabbit.oak.threadDump;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.sql.Timestamp;

/**
 * A tool that converts a file with date/time and thread dumps into a list of
 * date/time and just the thread names.
 */
public class ThreadDumpThreadNames {

    public static File process(File file) throws IOException {
        String fileName = file.getName();
        if (fileName.endsWith(".txt")) {
            fileName = fileName.substring(0, fileName.length() - ".txt".length());
        }
        File target = new File(file.getParentFile(), fileName + ".threadNames.txt");
        PrintWriter writer = new PrintWriter(new BufferedWriter(
                new FileWriter(target)));
        try {
            processFile(file, writer);
        } finally {
            writer.close();
        }
        return target;
    }

    private static void processFile(File file, PrintWriter writer) throws IOException {
        LineNumberReader r = new LineNumberReader(new BufferedReader(
                new FileReader(file)));
        try {
            process(r, writer);
        } finally {
            r.close();
        }
    }

    private static void process(LineNumberReader reader, PrintWriter writer) throws IOException {
        String dateAndTime = "";
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            if (line.length() == "2017-06-29 17:23:17".length()) {
                boolean isDateTime = true;
                for(char c : line.toCharArray()) {
                    if (!Character.isDigit(c) && " -:".indexOf(c) < 0) {
                        isDateTime = false;
                        break;
                    }
                }
                if (isDateTime) {
                    dateAndTime = line;
                    continue;
                }
            }
            if (line.startsWith("\"")) {
                line = line.substring(0, line.lastIndexOf('\"') + 1);
                line = appendTimestamp(line);
                writer.write(dateAndTime + " " + line + "\n");
            }
        }
    }

    private static String appendTimestamp(String line) {
        int start = line.indexOf("[");
        if (start < 0) {
            return line;
        }
        int end = line.indexOf("]", start);
        if (end < 0) {
            return line;
        }
        String millis = line.substring(start + 1, end);
        try {
            long m = Long.parseLong(millis);
            String t = new Timestamp(m).toString();
            return line + ": " + t;
        } catch (NumberFormatException e) {
            return line;
        }
    }

}
