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
package org.apache.jackrabbit.oak.index.merge;

import static java.util.Arrays.asList;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.LZ4Compression;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils;
import org.apache.jackrabbit.oak.index.indexer.document.tree.TreeStore;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.FilePacker;
import org.apache.jackrabbit.oak.run.commons.Command;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class IndexStoreCommand implements Command {
    
    public final static String INDEX_STORE = "index-store";

    @SuppressWarnings("unchecked")
    @Override
    public void execute(String... args) throws IOException {
        OptionParser parser = new OptionParser();
        OptionSpec<?> helpSpec = parser.acceptsAll(
                asList("h", "?", "help"), "show help").forHelp();
        OptionSet options = parser.parse(args);
        parser.nonOptions(
                "An index store file").ofType(File.class);        
        if (options.has(helpSpec)
                || options.nonOptionArguments().isEmpty()) {
            System.out.println("Mode: " + INDEX_STORE);
            System.out.println();
            parser.printHelpOn(System.out);
            return;
        }
        for (String fileName : ((List<String>) options.nonOptionArguments())) {
            File file = new File(fileName);
            if (!file.exists()) {
                System.out.println("File not found: " + fileName);
                return;
            }
            if (FilePacker.isPackFile(file)) {
                File treeFile = new File(file.getAbsoluteFile().getParent(), "tree");
                treeFile.mkdirs();
                FilePacker.unpack(file, treeFile, false);
                file = treeFile;
            }
            if (file.isDirectory()) {
                System.out.println("Tree store " + fileName);
                listTreeStore(file);
            } else if (file.isFile()) {
                System.out.println("Flat file " + fileName);
                listFlatFile(file);
            }
        }
    }
    
    public void listFlatFile(File file) throws IOException {
        BufferedReader reader;
        if (file.getName().endsWith(".lz4")) {
            reader = IndexStoreUtils.createReader(file, new LZ4Compression());
        } else if (file.getName().endsWith(".gz")) {
            reader = IndexStoreUtils.createReader(file, Compression.GZIP);
        } else {
            reader = IndexStoreUtils.createReader(file, Compression.NONE);
        }
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            System.out.println(line);
        }
        reader.close();
    }

    public static void listTreeStore(File directory) throws IOException {
        TreeStore treeStore = new TreeStore("tree", directory, null, 1);
        Iterator<String> it = treeStore.iteratorOverPaths();
        while (it.hasNext()) {
            String path = it.next();
            String node = treeStore.getSession().get(path);
            System.out.println(path + "|" + node);
        }
        treeStore.close();
    }

}
