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

/**
 * A tool that converts full thread dumps files to the "standard" format.
 */
public class ThreadDumpConverter {
    
    public static void main(String... args) throws IOException {
        process(new File(args[0]));
    }
    
    public static File process(File file) throws IOException {
        String fileName = file.getName();
        if (fileName.endsWith(".txt")) {
            fileName = fileName.substring(0, fileName.length() - ".txt".length());
        }
        File target = new File(file.getParentFile(), fileName + ".converted.txt");
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
        StringBuilder buff = new StringBuilder();
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            if (line.startsWith("    at ")) {
                line = "\t" + line.substring(4);
            } else if (line.startsWith("    Locked synchronizers")) {
                line = "   " + line.substring(4);
            } else if (line.startsWith("      - locked ")) {
                line = "\t" + line.substring(4);
            } else if (line.startsWith("     owned by ")) {
                line = "\t" + line.substring(4);
            }
            buff.append(line).append('\n');
            if (line.trim().length() == 0) {
                writer.print(convert(buff.toString()));
                buff = new StringBuilder();
            }
        }
        writer.println(convert(buff.toString()));
    }

    private static String convert(String string) {
        int endOfLine = string.indexOf('\n');
        String firstLine;
        if (endOfLine < 0) {
            firstLine = string;
        } else {
            firstLine = string.substring(0, endOfLine);
        }
        if (!firstLine.startsWith("\"")) {
            return string;
        }
        String remainingText = string.substring(endOfLine);
        for (Thread.State state : Thread.State.values()) {
            int index = firstLine.indexOf(" in " + state.toString());
            if (index > 0) {
                String stateName = firstLine.substring(index + 4, 
                        index + 4 + state.toString().length());
                firstLine = firstLine.substring(0, index) + "\n";
                remainingText = "   java.lang.Thread.State: "+ stateName + 
                        remainingText;
            }
        }
        return firstLine + remainingText;
    }

}
