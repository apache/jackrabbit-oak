/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.luceneNg.directory;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

/**
 * Serves unchanged segment reads from a prefetched snapshot and writes directly to Oak storage.
 */
public class ReadThroughDirectory extends FilterDirectory {
    private final Directory cached;
    private final Set<String> cachedNames = ConcurrentHashMap.newKeySet();

    public ReadThroughDirectory(OakDirectory remote, Directory cached) throws IOException {
        super(remote);
        this.cached = cached;
        Collections.addAll(cachedNames, cached.listAll());
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return cachedNames.contains(name) ? cached.openInput(name, context) : in.openInput(name, context);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        cachedNames.remove(name);
        return in.createOutput(name, context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        IndexOutput output = in.createTempOutput(prefix, suffix, context);
        cachedNames.remove(output.getName());
        return output;
    }

    @Override
    public void deleteFile(String name) throws IOException {
        cachedNames.remove(name);
        in.deleteFile(name);
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        cachedNames.remove(source);
        cachedNames.remove(dest);
        in.rename(source, dest);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(cached, in);
    }
}
