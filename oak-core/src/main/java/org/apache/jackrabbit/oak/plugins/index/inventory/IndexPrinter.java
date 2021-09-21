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

package org.apache.jackrabbit.oak.plugins.index.inventory;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import org.apache.felix.inventory.Format;
import org.apache.felix.inventory.InventoryPrinter;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfo;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexInfo;
import org.apache.jackrabbit.oak.plugins.index.IndexInfoService;
import org.apache.jackrabbit.util.ISO8601;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

import static com.google.common.base.Preconditions.checkNotNull;

@Component(
        service = InventoryPrinter.class,
        property = {
                "felix.inventory.printer.name=oak-index-stats",
                "felix.inventory.printer.title=Oak Index Stats",
                "felix.inventory.printer.format=TEXT",
                "felix.inventory.printer.format=JSON"
        })
public class IndexPrinter implements InventoryPrinter {

    @Reference
    private IndexInfoService indexInfoService;

    @Reference
    private AsyncIndexInfoService asyncIndexInfoService;

    public IndexPrinter() {
    }

    public IndexPrinter(IndexInfoService indexInfoService, AsyncIndexInfoService asyncIndexInfoService) {
        this.indexInfoService = checkNotNull(indexInfoService);
        this.asyncIndexInfoService = checkNotNull(asyncIndexInfoService);
    }

    @Override
    public void print(PrintWriter pw, Format format, boolean isZip) {

        if (format == Format.JSON) {
            JsopBuilder json = new JsopBuilder();
            startJsonObject(json);
            printAsyncIndexInfo(pw, json, format);
            printIndexInfo(pw, json, format);
            endJsonObject(json);
            pw.print(JsopBuilder.prettyPrint(json.toString()));
        } else {
            //TODO Highlight if failing
            printAsyncIndexInfo(pw, null, format);
            printIndexInfo(pw, null, format);
        }
    }

    private void printAsyncIndexInfo(PrintWriter pw, JsopBuilder json, Format format) {
        List<String> asyncLanes = ImmutableList.copyOf(asyncIndexInfoService.getAsyncLanes());
        String title = "Async Indexers State";
        printTitle(pw, title, format);
        addJsonKey(json, title);
        startJsonObject(json);
        keyValue("Number of async indexer lanes ", asyncLanes.size(), pw, json, format);
        printWithNewLine(pw, "", format);

        for (String lane : asyncLanes) {
            printWithNewLine(pw, lane, format);
            addJsonKey(json, lane);
            AsyncIndexInfo info = asyncIndexInfoService.getInfo(lane);
            startJsonObject(json);
            if (info != null) {
                keyValue("    Last indexed to      ", formatTime(info.getLastIndexedTo()), pw, json, format);
                IndexStatsMBean stats = info.getStatsMBean();
                if (stats != null) {
                    keyValue("    Status              ", stats.getStatus(), pw, json, format);
                    keyValue("    Failing             ", stats.isFailing(), pw, json, format);
                    keyValue("    Paused              ", stats.isPaused(), pw, json, format);
                    if (stats.isFailing()) {
                        keyValue("    Failing since       ", stats.getFailingSince(), pw, json, format);
                        keyValue("    Latest error        ", stats.getLatestError(), pw, json, format);
                    }
                }
                printWithNewLine(pw, "", format);
            }
            endJsonObject(json);
        }
        endJsonObject(json);
    }

    private static void printTitle(PrintWriter pw, String title, Format format) {
        if (format == Format.TEXT) {
            pw.println(title);
            pw.println(Strings.repeat("=", title.length()));
            pw.println();
        }
    }

    private static void printWithNewLine(PrintWriter pw, String printLine, Format format) {
        if (format == Format.TEXT) {
            pw.println(printLine);
        }
    }

    private void printIndexInfo(PrintWriter pw, JsopBuilder json, Format format) {
        ListMultimap<String, IndexInfo> infos = ArrayListMultimap.create();
        for (IndexInfo info : indexInfoService.getAllIndexInfo()) {
            infos.put(info.getType(), info);
        }

        keyValue("Total number of indexes ", infos.size(), pw, json, format);

        for (String type : infos.keySet()){
            List<IndexInfo> typedInfo = infos.get(type);
            String title = String.format("%s(%d)", type, typedInfo.size());
            printTitle(pw, title, format);
            addJsonKey(json, type);
            startJsonObject(json);
            for (IndexInfo info : typedInfo){
                printIndexInfo(pw, json, info, format);
            }
            endJsonObject(json);
        }
    }


    private static void printIndexInfo(PrintWriter pw, JsopBuilder json, IndexInfo info, Format format) {
        if (format == Format.TEXT) {
            pw.println(info.getIndexPath());
        }
        addJsonKey(json, info.getIndexPath());
        startJsonObject(json);

        keyValue("    Type                    ", info.getType(), pw, json, format);
        if (info.getAsyncLaneName() != null) {
            keyValue("    Async                    ", true, pw, json, format);
            keyValue("    Async lane name          ", info.getAsyncLaneName(), pw, json, format);
        }

        if (info.getIndexedUpToTime() > 0){
            keyValue("    Last indexed up to       ", formatTime(info.getIndexedUpToTime()), pw, json, format);
        }

        if (info.getLastUpdatedTime() > 0){
            keyValue("     Last updated time       ", formatTime(info.getLastUpdatedTime()), pw, json, format);
        }

        if (info.getCreationTimestamp() > 0){
            keyValue("     Creation time           ", formatTime(info.getCreationTimestamp()), pw, json, format);
        }

        if (info.getReindexCompletionTimestamp() > 0){
            keyValue("     Reindex completion time ", formatTime(info.getReindexCompletionTimestamp()), pw, json, format);
        }

        if (info.getSizeInBytes() >= 0){
            keyValue("    Size                     ", IOUtils.humanReadableByteCount(info.getSizeInBytes()), pw, json, format);
            keyValue("    Size (in Bytes)          ", info.getSizeInBytes(), pw, json, format);
        }

        if (info.getSuggestSizeInBytes() >= 0){
            keyValue("    Suggest size             ", IOUtils.humanReadableByteCount(info.getSuggestSizeInBytes()), pw, json, format);
            keyValue("    Suggest size (in Bytes)  ", info.getSuggestSizeInBytes(), pw, json, format);
        }

        if (info.getEstimatedEntryCount() >= 0){
            keyValue("    Estimated entry count    ", info.getEstimatedEntryCount(), pw, json, format);
        }

        if ("lucene".equals(info.getType())) {
            // Only valid for lucene type indexes, for others it will simply show false.
            keyValue("    Has hidden oak mount     ", info.hasHiddenOakLibsMount(), pw, json, format);
            keyValue("    Has property index       ", info.hasPropertyIndexNode(), pw, json, format);
        }

        if (info.hasIndexDefinitionChangedWithoutReindexing()) {
            String diff = info.getIndexDefinitionDiff();
            if (diff != null) {
                keyValue("    Index definition changed without reindexing ", diff, pw, json, format);
                printWithNewLine(pw, "", format);
            }
        }
        endJsonObject(json);
    }

    private static String formatTime(long time){
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        Date date = cal.getTime();
        SimpleDateFormat outputFmt = new SimpleDateFormat("yyyy-mm-dd'T'HH:mm:ss.s z");
        outputFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        return outputFmt.format(date);
    }

    private static void keyValue(String key, Object value, PrintWriter pw, JsopBuilder json, Format format) {
        // In case the key is null or an emppty String or value is null,
        // throw an IllegalArgumentException.
        if (value == null || key == null || key.equals("")) {
            throw new IllegalArgumentException("Unsupported key/value pair - can't be null/empty");
        }

        if (format == Format.JSON) {
            json.key(key.trim());
            if (value instanceof  String) {
                json.value((String)value);
            } else if (value instanceof  Long) {
                json.value((Long)value);
            } else if (value instanceof  Boolean) {
                json.value((Boolean)value);
            } else if (value instanceof Integer) {
                json.value((Integer) value);
            } else {
                throw new IllegalArgumentException("Unsupported type of value while creating the json output");
            }
        } else if (format == Format.TEXT) {
            pw.printf(key+":%s%n",value);
        }
    }

    // Wrappers around JsonBuilder that will result in NOOP if builder is null -
    // These are just to avoid the multiple if/else check while handling for both TEXT and JSON formats
    private static void startJsonObject(JsopBuilder json) {
        if (json != null) {
            json.object();
        }
    }

    private static void endJsonObject(JsopBuilder json) {
        if (json != null) {
            json.endObject();
        }
    }

    private static void addJsonKey(JsopBuilder json, String key) {
        if (json != null) {
            json.key(key);
        }
    }
}
