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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.jackrabbit.oak.commons.Profiler;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.threadDump.ThreadDumpThreadNames;
import org.apache.jackrabbit.oak.threadDump.ThreadDumpCleaner;
import org.apache.jackrabbit.oak.threadDump.ThreadDumpConverter;

public class ThreadDumpCommand implements Command {
    public final static String THREADDUMP = "threaddump";

    @SuppressWarnings("unchecked")
    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<Void> convertSpec = parser.accepts("convert",
                "convert the thread dumps to the standard format");
        OptionSpec<Void> filterSpec = parser.accepts("filter",
                "filter the thread dumps, only keep working (running), interesting threads " +
                "(for example, threads that read from sockets are ignored, " +
                "as they are typically waiting for input; " +
                 "system threads such as GC are also ignored)");
        OptionSpec<Void> threadNamesSpec = parser.accepts("threadNames",
                "create a summary of thread names");
        OptionSpec<Void> profileSpec = parser.accepts("profile",
                "profile the thread dumps");
        OptionSpec<Void> profileClassesSpec = parser.accepts("profileClasses",
                "profile classes");
        OptionSpec<Void> profileMethodsSpec = parser.accepts("profileMethods",
                "profile methods");
        OptionSpec<Void> profilePackagesSpec = parser.accepts("profilePackages",
                "profile packages");
        OptionSpec<?> helpSpec = parser.acceptsAll(
                asList("h", "?", "help"), "show help").forHelp();
        OptionSet options = parser.parse(args);
        parser.nonOptions(
                "file or directory containing thread dumps " +
                "(ensure it does not contain other files, such as binaries)").ofType(File.class);
        if (options.has(helpSpec)
                || options.nonOptionArguments().isEmpty()) {
            System.out.println("Mode: " + THREADDUMP);
            System.out.println();
            parser.printHelpOn(System.out);
            return;
        }
        boolean convert = options.has(convertSpec);
        boolean filter = options.has(filterSpec);
        boolean threadNames = options.has(threadNamesSpec);
        boolean profile = options.has(profileSpec);
        boolean profileClasses = options.has(profileClassesSpec);
        boolean profileMethods = options.has(profileMethodsSpec);
        boolean profilePackages = options.has(profilePackagesSpec);
        for (String fileName : ((List<String>) options.nonOptionArguments())) {
            File file = new File(fileName);
            if (file.isDirectory() || file.getName().endsWith(".gz")) {
                file = combineAndExpandFiles(file);
                System.out.println("Combined into " + file.getAbsolutePath());
            }
            if (convert) {
                file = ThreadDumpConverter.process(file);
                System.out.println("Converted to " + file.getAbsolutePath());

            }
            if (threadNames) {
                File f = ThreadDumpThreadNames.process(file);
                System.out.println("Thread names written to " + f.getAbsolutePath());
            }
            if (filter) {
                file = ThreadDumpCleaner.process(file);
                System.out.println("Filtered into " + file.getAbsolutePath());
            }
            if (threadNames) {
                File f = ThreadDumpThreadNames.process(file);
                System.out.println("Thread names written to " + f.getAbsolutePath());
            }
            if (profile) {
                ArrayList<String> list = new ArrayList<String>();
                if (profileClasses) {
                    list.add("-classes");
                }
                if (profileMethods) {
                    list.add("-methods");
                }
                if (profilePackages) {
                    list.add("-packages");
                }
                list.add(file.getAbsolutePath());
                Profiler.main(list.toArray(new String[0]));
            }
        }
    }

    private static File combineAndExpandFiles(File file) throws IOException {
        if (!file.exists() || !file.isDirectory()) {
            if (!file.getName().endsWith(".gz")) {
                return file;
            }
        }
        File target = new File(file.getParentFile(), file.getName() + ".txt");
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(
                target)));
        try {
            int count = processFileOrDirectory(file, writer);
            System.out.println("    (total " + count + " full thread dumps)");
        } finally {
            writer.close();
        }
        return target;
    }

    private static int processFileOrDirectory(File file, PrintWriter writer)
            throws IOException {
        if (file.isFile()) {
            return processFile(file, writer);
        }
        int count = 0;
        File[] list = file.listFiles();
        if (list != null) {
            for(File f : list) {
                count += processFileOrDirectory(f, writer);
            }
        }
        return count;
    }

    private static int processFile(File file, PrintWriter writer) throws IOException {
        Reader reader = null;
        int count = 0;
        try {
            if (file.getName().endsWith(".DS_Store")) {
                return 0;
            }
            int fullThreadDumps = 0;
            String fileModifiedTime = new Timestamp(file.lastModified()).toString();
            writer.write("file " + file.getAbsolutePath() + "\n");
            writer.write("lastModified " + fileModifiedTime + "\n");
            if (file.getName().endsWith(".gz")) {
                System.out.println("Extracting " + file.getAbsolutePath());
                InputStream fileStream = new FileInputStream(file);
                try {
                    InputStream gzipStream = new GZIPInputStream(fileStream);
                    reader = new InputStreamReader(gzipStream);
                } catch (EOFException e) {
                    fileStream.close();
                    return 0;
                }
            } else {
                System.out.println("Reading " + file.getAbsolutePath());
                reader = new FileReader(file);
            }
            LineNumberReader in = new LineNumberReader(new BufferedReader(
                    reader));
            while (true) {
                String s;
                try {
                    s = in.readLine();
                } catch (EOFException e) {
                    // EOFException: Unexpected end of ZLIB input stream
                    break;
                } catch (ZipException e) {
                    // java.util.zip.ZipException: invalid block type
                    break;
                }
                if (s == null) {
                    break;
                }
                if (s.startsWith("Full thread dump") || s.startsWith("Full Java thread dump")) {
                    fullThreadDumps++;
                }
                writer.println(s);
            }
            if (fullThreadDumps > 0) {
                count++;
                System.out.println("    (contains " + fullThreadDumps + " full thread dumps; " + fileModifiedTime + ")");
            }
        } finally {
            if(reader != null) {
                reader.close();
            }
        }
        return count;
    }

}
