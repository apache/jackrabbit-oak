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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property.ValueType;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.NodeCount;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules.PropertyStats;

import net.jpountz.lz4.LZ4FrameInputStream;

public class Reader {
    public static void main(String... args) throws IOException {
        String fileName = args[0];
        long fileSize = new File(fileName).length();
        LZ4FrameInputStream in = new LZ4FrameInputStream(new BufferedInputStream(new FileInputStream(fileName)));
        LineNumberReader reader = new LineNumberReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        long start = System.nanoTime();
        
        ArrayList<StatsCollector> collectors = new ArrayList<>();
        
        collectors.add(new NodeCount(1000));
        collectors.add(new PropertyStats());
//        collectors.add(new BinarySize(1));
//        collectors.add(new BinarySize(100_000_000));
//        collectors.add(new BinarySizeEmbedded(1));
//        collectors.add(new BinarySizeEmbedded(100_000));
//        collectors.add(new BinarySizeHistogram(1));
//        collectors.add(new TopLargestBinaries(10));
//        collectors.add(new PathFilter("cqdam.text.txt", new BinarySize(1)));
//        collectors.add(new PathFilter("cqdam.text.txt", new BinarySize(100_000_000)));
//        collectors.add(new PathFilter("cqdam.text.txt", new BinarySizeEmbedded(1)));
//        collectors.add(new PathFilter("cqdam.text.txt", new BinarySizeEmbedded(100_000)));
//        collectors.add(new PathFilter("cqdam.text.txt", new BinarySizeHistogram(1)));
//        collectors.add(new PathFilter("cqdam.text.txt", new TopLargestBinaries(10)));
        
        for(StatsCollector collector : collectors) {
            collector.setStorage(new Storage());
        }
        
        int count = 0;
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            if (count < 10) {
            //    System.out.println(line);
            }
            if (count % 1000000 == 0) {
                System.out.println(count + " lines");
            }
            int pipeIndex = line.indexOf('|');
            if (pipeIndex < 0) {
                System.out.println("Error: no pipe: " + line);
                continue;
            }
            String path = line.substring(0, pipeIndex);
            List<String> pathElements = new ArrayList<>();
            PathUtils.elements(path).forEach(pathElements::add);
            String nodeData = line.substring(pipeIndex + 1);
            List<Property> properties = parse(nodeData);
            
            for(StatsCollector collector : collectors) {
                collector.add(pathElements, properties);
            }
            
            count++;
            if (count > 1000000) {
                break;
            }
        }
        
        for(StatsCollector collector : collectors) {
            collector.end();
        }

        for(StatsCollector collector : collectors) {
            System.out.println(collector.toString());
        }

        System.out.println(count + " lines total");
        long time = System.nanoTime() - start;
        System.out.println((time / 1_000_000_000) + " seconds");
        System.out.println((time / count) + " ns/entry");
        System.out.println(fileSize + " bytes");
        System.out.println((fileSize / count) + " bytes/entry");
        in.close();
    }

    private static List<Property> parse(String nodeData) {
        ArrayList<Property> properties = new ArrayList<>();
        JsonObject json = JsonObject.fromJson(nodeData, true);
        for(Entry<String, String> e : json.getProperties().entrySet()) {
            String v = e.getValue();
            Property p;
            if (v.startsWith("[")) {
                p = fromJsonArray(e.getKey(), v);
            } else if (v.startsWith("\"")) {
                String v2 = JsopTokenizer.decodeQuoted(v);
                p = new Property(e.getKey(), ValueType.STRING, v2);
            } else if ("null".equals(v)) {
                p = new Property(e.getKey(), ValueType.NULL, "");
            } else if ("true".equals(v)) {
                p = new Property(e.getKey(), ValueType.BOOLEAN, v);
            } else if ("false".equals(v)) {
                p = new Property(e.getKey(), ValueType.BOOLEAN, v);
            } else {
                p = new Property(e.getKey(), ValueType.NUMBER, v);
            }
            properties.add(p);
        }
        if (!json.getChildren().isEmpty()) {
            throw new IllegalArgumentException("Unexpected children " + json.getChildren());
        }
        return properties;
    }
    
    public static Property fromJsonArray(String key, String json) {
        ArrayList<String> result = new ArrayList<>();
        ValueType type = null;
        JsopTokenizer tokenizer = new JsopTokenizer(json);
        tokenizer.read('[');
        if (!tokenizer.matches(']')) {
            do {
                switch (tokenizer.read()) {
                case JsopTokenizer.STRING:
                    if (type != null && type != ValueType.STRING) {
                        throw new IllegalArgumentException("Unsupported mixed type: " + json);
                    }
                    type = ValueType.STRING;
                    String v = tokenizer.getToken();
                    result.add(v);
                    break;
                case JsopTokenizer.NUMBER:
                    if (type != null && type != ValueType.NUMBER) {
                        throw new IllegalArgumentException("Unsupported mixed type: " + json);
                    }
                    type = ValueType.NUMBER;
                    result.add(tokenizer.getToken());
                    break;
                case JsopTokenizer.FALSE:
                case JsopTokenizer.TRUE:
                    if (type != null && type != ValueType.BOOLEAN) {
                        throw new IllegalArgumentException("Unsupported mixed type: " + json);
                    }
                    type = ValueType.BOOLEAN;
                    result.add(tokenizer.getToken());
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported type: " + json);
                }                    
            } while (tokenizer.matches(','));
            tokenizer.read(']');
        }
        tokenizer.read(JsopReader.END);
        return new Property(key, type, result.toArray(new String[result.size()]), true);
    }    
}
