/* Licensed to the Apache Software Foundation (ASF) under one or more
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
package org.apache.jackrabbit.oak.plugins.index.lucene.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.LocalIndexDir;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReader;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.BytesRef;

/**
 * Tool to compare Lucene indexes.
 * Indexes are considered equal if everything except the following is equal:
 * - checksums of .si, .cfe, .cfs files
 * - size of .si, .cfe, .cfs files (up to 1000 bytes or 0.1% difference is acceptable)
 */
public class IndexComparator {

    public static void main(String... args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: java " + IndexComparator.class.getName() +
                    " <lucene index directory> [<second lucene index directory>]");
            return;
        }
        String stats1 = getStats(new File(args[0]));
        if (args.length > 1) {
            String stats2 = getStats(new File(args[1]));
            System.out.println(diff(stats1, stats2));
        } else {
            System.out.println(stats1);
        }
    }

    private static String diff(String stats1, String stats2) {
        JsonObject o1 = JsonObject.fromJson(stats1, true);
        JsonObject o2 = JsonObject.fromJson(stats2, true);
        o1.getProperties().remove("directory");
        o2.getProperties().remove("directory");
        JsopBuilder result = new JsopBuilder();
        result.object();
        diff(o1, o2, "", result);
        result.endObject();
        if (result.toString().equals("{}")) {
            return "";
        }
        return JsopBuilder.prettyPrint(result.toString());
    }

    private static void diff(JsonObject o1, JsonObject o2, String path, JsopBuilder result) {
        boolean ignoreChecksums = false;
        if (path.startsWith("/files/")) {
            if (path.endsWith("index-details.txt")) {
                // ignore index details
                ignoreChecksums = true;
            } else if (path.endsWith(".si")) {
                // ".si" files contain a timestamp
                ignoreChecksums = true;
            } else if (path.endsWith(".cfs")) {
                // ".cfs" files depends on the aggregation order
                ignoreChecksums = true;
            } else if (path.endsWith(".cfe")) {
                // ".cfs" files depends on the aggregation order
                ignoreChecksums = true;
            }
        }
        Set<String> both = new HashSet<>(o1.getProperties().keySet());
        both.addAll(o2.getProperties().keySet());
        for (String k : both) {
            String v1 = o1.getProperties().get(k);
            String v2 = o2.getProperties().get(k);
            if (v1 == null || v2 == null || !v1.equals(v2)) {
                if (ignoreChecksums) {
                    if ("sha256".equals(k) || "md5".equals(k)) {
                        continue;
                    }
                    if ("bytes".equals(k) && v1 != null && v2 != null) {
                        long x1 = Long.parseLong(v1);
                        long x2 = Long.parseLong(v2);
                        if (Math.abs(x1 - x2) < 1000) {
                            // less than 0.1% difference if aggregation order is different
                            continue;
                        } else if (Math.abs((double) x1 / x2 - 1.0) < 0.001) {
                            // less than 0.1% difference if aggregation order is different
                            continue;
                        }
                    }
                }
                result.key(path + "/" + k).object().key("v1").encodedValue(v1).key("v2").encodedValue(v2).endObject();
            }
        }
        both = new HashSet<>(o1.getChildren().keySet());
        both.addAll(o2.getChildren().keySet());
        for (String k : both) {
            JsonObject v1 = o1.getChildren().get(k);
            JsonObject v2 = o2.getChildren().get(k);
            if (v1 == null || v2 == null) {
                result.key(path + "/" + k).object().key("v1").encodedValue(v1.toString()).key("v2").encodedValue(v2.toString()).endObject();
            } else {
                diff(v1, v2, path + "/" + k, result);
            }
        }
    }

    private static String getStats(File file) throws Exception {
        if (file.isDirectory()) {
            return getStatsFromLucene(file);
        } else {
            return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        }
    }

    private static String getStatsFromLucene(File directory) throws Exception {
        LocalIndexDir dir = new LocalIndexDir(directory);
        JsopBuilder builder = new JsopBuilder();
        builder.object();
        builder.key("directory").value(directory.getAbsolutePath());
        builder.key("files").object();
        fileStats(directory, builder);
        builder.endObject();
        builder.key("luceneIndex").object();
        builder.key("jcrPath").value(dir.getJcrPath());
        SimpleFSDirectory luceneDir = new SimpleFSDirectory(new File(directory, "data"));
        SimpleFSDirectory suggestDir = null;
        if (new File(directory, "suggest-data").exists()) {
            suggestDir = new SimpleFSDirectory(new File(directory, "suggest-data"));
        }
        try (LuceneIndexReader luceneReader = new DefaultIndexReader(luceneDir, suggestDir, null)) {
            IndexReader reader = luceneReader.getReader();
            builder.key("numDocs").value(reader.numDocs());
            builder.key("maxDoc").value(reader.maxDoc());
            builder.key("numDeletedDocs").value(reader.numDeletedDocs());

            Fields fields = MultiFields.getFields(reader);
            if (fields != null) {
                builder.key("fields").object();
                for (String f : fields) {
                    builder.key(f).object();
                    builder.key("docCount").value(reader.getDocCount(f));
                    Terms terms = MultiFields.getTerms(reader, f);
                    TermsEnum iterator = terms.iterator(null);
                    BytesRef byteRef = null;
                    Function<BytesRef, String> handler = BytesRef::utf8ToString;
                    int termCount = 0;
                    int termHash = 0;
                    long docFreqSum = 0;
                    while ((byteRef = iterator.next()) != null) {
                        termCount++;
                        String term = handler.apply(byteRef);
                        termHash ^= term.hashCode();
                        int docFreq = iterator.docFreq();
                        docFreqSum += docFreq;
                    }
                    builder.key("termCount").value(termCount);
                    builder.key("termHash").value(termHash);
                    builder.key("docFreqSum").value(docFreqSum);
                    builder.endObject();
                }
                builder.endObject();
            }
        }
        builder.endObject();
        builder.endObject();
        return JsopBuilder.prettyPrint(builder.toString());
    }

    private static void fileStats(File file, JsopBuilder builder) throws IOException, NoSuchAlgorithmException {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                fileStats(f, builder);
            }
        } else {
            builder.key(file.getName()).object();
            builder.key("bytes").value(file.length());
            try(FileInputStream in = new FileInputStream(file)) {
                MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
                MessageDigest md5 = MessageDigest.getInstance("MD5");
                byte[] buffer = new byte[8 * 1024];
                int numOfBytesRead;
                while( (numOfBytesRead = in.read(buffer)) > 0){
                    sha256.update(buffer, 0, numOfBytesRead);
                    md5.update(buffer, 0, numOfBytesRead);
                }
                builder.key("sha256").value(toHex(sha256.digest()));
                builder.key("md5").value(toHex(md5.digest()));
            }
            builder.endObject();
        }
    }

    private static String toHex(byte[] data) {
        StringBuilder buff = new StringBuilder();
        for(byte b: data) {
            buff.append(String.format("%02x", b));
        }
        return buff.toString();
    }
}
