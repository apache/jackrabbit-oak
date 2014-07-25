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

package org.apache.jackrabbit.oak.console.commands

import com.google.common.base.Stopwatch
import org.apache.jackrabbit.oak.commons.PathUtils
import org.apache.jackrabbit.oak.plugins.index.lucene.OakDirectory
import org.apache.jackrabbit.oak.spi.state.NodeState
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.store.IOContext
import org.codehaus.groovy.tools.shell.ComplexCommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME

class LuceneCommand extends ComplexCommandSupport {
    public static final String COMMAND_NAME = 'lucene'

    public LuceneCommand(final Groovysh shell) {
        super(shell, COMMAND_NAME, 'lc', ['info', 'dump'], 'info')
    }

    def do_info = { args ->
        String idxPath = args ? args[0] : '/oak:index/lucene'
        DirectoryReader reader = DirectoryReader.open(getDirectory(idxPath))
        if (!reader) {
            io.out.println("No Lucene directory found at path [$idxPath]")
            return
        }
        try {
            io.out.println("Number of documents : ${reader.numDocs()}")
            io.out.println("Number of deleted documents : ${reader.numDeletedDocs()}")
            io.out.println("Index size : ${humanReadableByteCount(dirSize(reader.directory()))}")
        } finally {
            reader.close()
        }
    }

    def do_dump = { args ->
        String idxPath = args && args.size() == 2 ? args[1] : '/oak:index/lucene'
        String destPath = args ? args[0] : 'luceneIndex'
        Directory source = getDirectory(idxPath)
        if (!source) {
            io.out.println("No Lucene directory found at path [$idxPath]")
            return
        }
        try {
            File destDir = new File(destPath)
            Stopwatch w = Stopwatch.createStarted()
            io.out.println("Copying Lucene indexes to [${destDir.absolutePath}]")
            Directory dest = FSDirectory.open(destDir)
            long size = 0
            source.listAll().each { file ->
                size += source.fileLength(file)
                source.copy(dest, file, file, IOContext.DEFAULT)
            }
            io.out.println("Copied ${humanReadableByteCount(size)} in $w")
        } finally {
            source.close()
        }
    }

    private Directory getDirectory(String path) {
        NodeState definition = variables.session.getRoot();
        for (String element : PathUtils.elements(path)) {
            definition = definition.getChildNode(element);
        }
        NodeState data = definition.getChildNode(INDEX_DATA_CHILD_NAME);
        if (data.exists()) {
            //OakDirectory is package scope but Groovy allows us
            //to use it. Good or bad but its helpful debug scripts
            //can access inner classes and prod code cannot. Win win :)
            return new OakDirectory(new ReadOnlyBuilder(data));
        }
        return null
    }

    private static long dirSize(Directory dir) {
        dir.listAll().inject(0L, { sum, fileName -> sum + dir.fileLength(fileName) })
    }

    private static def humanReadableByteCount(long bytes) {
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp - 1);
        return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
    }
}
