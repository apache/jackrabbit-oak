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
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.felix.inventory.Format;
import org.apache.felix.inventory.InventoryPrinter;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfo;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexInfo;
import org.apache.jackrabbit.oak.plugins.index.IndexInfoService;
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
        PrinterOutput po = format == Format.JSON ? new JsonPrinterOutput() : new TextPrinterOutput();
        asyncLanesInfo(po);
        indexInfo(po);
        pw.print(po.output());
    }

    private void asyncLanesInfo(PrinterOutput po) {
        List<String> asyncLanes = ImmutableList.copyOf(asyncIndexInfoService.getAsyncLanes());
        po.startSection("Async Indexers State", true);
        po.text("Number of async indexer lanes", asyncLanes.size());

        for (String lane : asyncLanes) {
            po.startSection(lane, false);
            AsyncIndexInfo info = asyncIndexInfoService.getInfo(lane);
            if (info != null) {
                po.text("Last indexed to", formatTime(info.getLastIndexedTo()));
                IndexStatsMBean stats = info.getStatsMBean();
                if (stats != null) {
                    po.text("Status", stats.getStatus());
                    po.text("Failing", stats.isFailing());
                    po.text("Paused", stats.isPaused());
                    if (stats.isFailing()) {
                        po.text("Failing since", stats.getFailingSince());
                        po.text("Latest error", stats.getLatestError());
                    }
                }
            }
            po.endSection();
        }
        po.endSection();
    }

    private void indexInfo(PrinterOutput po) {
        Map<String, List<IndexInfo>> indexesByType = StreamSupport.stream(indexInfoService.getAllIndexInfo().spliterator(), false)
                .collect(Collectors.groupingBy(IndexInfo::getType));

        po.text("Total number of indexes", indexesByType.values().stream().mapToInt(List::size).sum());

        for (String type : indexesByType.keySet()) {
            List<IndexInfo> typedInfo = indexesByType.get(type);
            po.startSection(type, true);
            po.text("Number of " + type + " indexes", typedInfo.size());
            for (IndexInfo info : typedInfo) {
                indexInfo(po, info);
            }
            po.endSection();
        }
    }

    private void indexInfo(PrinterOutput po, IndexInfo info) {
        po.startSection(info.getIndexPath(), false);

        po.text("Type", info.getType());
        if (info.getAsyncLaneName() != null) {
            po.text("Async", true);
            po.text("Async lane name", info.getAsyncLaneName());
        }

        if (info.getIndexedUpToTime() > 0) {
            po.text("Last indexed up to", formatTime(info.getIndexedUpToTime()));
        }

        if (info.getLastUpdatedTime() > 0) {
            po.text("Last updated time", formatTime(info.getLastUpdatedTime()));
        }

        if (info.getCreationTimestamp() > 0) {
            po.text("Creation time", formatTime(info.getCreationTimestamp()));
        }

        if (info.getReindexCompletionTimestamp() > 0) {
            po.text("Reindex completion time", formatTime(info.getReindexCompletionTimestamp()));
        }

        if (info.getSizeInBytes() >= 0) {
            po.text("Size", IOUtils.humanReadableByteCount(info.getSizeInBytes()));
            po.text("Size (in bytes)", info.getSizeInBytes());
        }

        if (info.getSuggestSizeInBytes() >= 0) {
            po.text("Suggest size", IOUtils.humanReadableByteCount(info.getSuggestSizeInBytes()));
            po.text("Suggest size (in bytes)", info.getSuggestSizeInBytes());
        }

        if (info.getEstimatedEntryCount() >= 0) {
            po.text("Estimated entry count", info.getEstimatedEntryCount());
        }

        if ("lucene".equals(info.getType())) {
            // Only valid for lucene type indexes, for others it will simply show false.
            po.text("Has hidden oak mount", info.hasHiddenOakLibsMount());
            po.text("Has property index", info.hasPropertyIndexNode());
        }

        if (info.hasIndexDefinitionChangedWithoutReindexing()) {
            String diff = info.getIndexDefinitionDiff();
            if (diff != null) {
                po.text("Index definition changed without reindexing", diff);
            }
        }
        po.endSection();
    }

    private static String formatTime(long time) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        Date date = cal.getTime();
        SimpleDateFormat outputFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        outputFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        return outputFmt.format(date);
    }

    private static abstract class PrinterOutput {

        abstract String output();

        abstract void startSection(String section, boolean topLevel);

        void endSection() {
        }

        abstract void text(String key, Object value);

    }

    private static class JsonPrinterOutput extends PrinterOutput {

        private final JsopBuilder json;

        public JsonPrinterOutput() {
            this.json = new JsopBuilder().object();
        }

        @Override
        String output() {
            return JsopBuilder.prettyPrint(json.endObject().toString());
        }

        @Override
        void startSection(String section, boolean topLevel) {
            json.key(section);
            json.object();
        }

        @Override
        void endSection() {
            json.endObject();
        }

        @Override
        void text(String key, Object value) {
            json.key(key);
            if (value instanceof String) {
                json.value((String) value);
            } else if (value instanceof Long) {
                json.value((Long) value);
            } else if (value instanceof Boolean) {
                json.value((Boolean) value);
            } else if (value instanceof Integer) {
                json.value((Integer) value);
            } else {
                throw new IllegalArgumentException("Unsupported type of value while creating the json output");
            }
        }
    }

    private static class TextPrinterOutput extends PrinterOutput {

        private final StringBuilder sb;

        private int leftPadding = 0;

        public TextPrinterOutput() {
            this.sb = new StringBuilder();
        }

        @Override
        String output() {
            return sb.toString();
        }

        @Override
        void startSection(String section, boolean topLevel) {
            newLine();
            appendLine(section);
            if (topLevel) {
                appendLine(Strings.repeat("=", section.length()));
                newLine();
            } else {
                leftPadding += 4;
            }
        }

        @Override
        void endSection() {
            if (leftPadding >= 4) leftPadding -= 4;
        }

        @Override
        void text(String key, Object value) {
            appendLine(key, value);
        }

        private void appendLine(String text) {
            sb.append(text).append(System.lineSeparator());
        }

        private void appendLine(String key, Object value) {
            String leftPaddedKey = leftPadding > 0 ? String.format("%" + leftPadding + "s", key) : key;
            sb.append(String.format("%-29s", leftPaddedKey))
                    .append(":").append(value).append(System.lineSeparator());
        }

        private void newLine() {
            sb.append(System.lineSeparator());
        }
    }
}
