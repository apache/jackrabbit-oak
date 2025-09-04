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
package org.apache.jackrabbit.oak.index.indexer.document.tree.store;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

/**
 * A helper class to build storage backends for a tree store.
 */
public class StoreBuilder {

    /**
     * Build a store. The configuration options are passed as a list of properties.
     *
     * - empty string or null: in-memory.
     * - "type=memory"
     * - "type=file": file system, with "dir" directory
     * - "type=stats.another": statistics wrapper around another
     * - "type=slow.another": slow wrapper around another (to simulate slowness)
     * - "type=pack": pack file, with "file" file name
     * - "type=log.another": log wrapper around another (to analyze problems)
     * - "maxFileSizeBytes=16000000": the maximum file size in bytes
     *
     * @param config the config
     * @return a store
     * @throws IllegalArgumentException
     */
    public static Store build(String config) throws IllegalArgumentException {
        if (config == null || config.isEmpty()) {
            return new MemoryStore(new Properties());
        }
        config = config.replace("\\", "\\\\");
        Properties prop = new Properties();
        try {
            prop.load(new StringReader(config));
        } catch (IOException e) {
            throw new IllegalArgumentException(config, e);
        }
        return build(prop);
    }

    public static Store build(Properties config) {
        String type = config.getProperty("type");
        switch (type) {
        case "memory":
            return new MemoryStore(config);
        case "file":
            return new FileStore(config);
        case "pack":
            return new PackStore(config);
        }
        if (type.startsWith("stats.")) {
            config.put("type", type.substring("stats.".length()));
            return new StatsStore(build(config));
        } else if (type.startsWith("slow.")) {
            config.put("type", type.substring("slow.".length()));
            return new SlowStore(build(config));
        } else if (type.startsWith("log.")) {
            config.put("type", type.substring("log.".length()));
            return new LogStore(build(config));
        }
        throw new IllegalArgumentException(config.toString());
    }

    public static Properties subProperties(Properties p, String prefix) {
        Properties p2 = new Properties();
        for (Object k : p.keySet()) {
            if (k.toString().startsWith(prefix)) {
                p2.put(k.toString().substring(prefix.length()), p.get(k));
            }
        }
        return p2;
    }

}
