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
import java.util.Calendar;
import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import org.apache.felix.inventory.Format;
import org.apache.felix.inventory.InventoryPrinter;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.commons.IOUtils;
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
            "felix.inventory.printer.format=TEXT"
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
        //TODO Highlight if failing
        printAsyncIndexInfo(pw);
        printIndexInfo(pw);
    }

    private void printAsyncIndexInfo(PrintWriter pw) {
        List<String> asyncLanes = ImmutableList.copyOf(asyncIndexInfoService.getAsyncLanes());
        String title = "Async Indexers State";
        printTitle(pw, title);
        pw.printf("Number of async indexer lanes : %d%n", asyncLanes.size());
        pw.println();
        for (String lane : asyncLanes) {
            pw.println(lane);
            AsyncIndexInfo info = asyncIndexInfoService.getInfo(lane);
            if (info != null) {
                        pw.printf("    Last indexed to      : %s%n", formatTime(info.getLastIndexedTo()));
                IndexStatsMBean stats = info.getStatsMBean();
                if (stats != null) {
                        pw.printf("    Status              : %s%n", stats.getStatus());
                        pw.printf("    Failing             : %s%n", stats.isFailing());
                        pw.printf("    Paused              : %s%n", stats.isPaused());
                    if (stats.isFailing()) {
                        pw.printf("    Failing since       : %s%n", stats.getFailingSince());
                        pw.printf("    Latest error        : %s%n", stats.getLatestError());
                    }
                }
                pw.println();
            }
        }
    }

    private static void printTitle(PrintWriter pw, String title) {
        pw.println(title);
        pw.println(Strings.repeat("=", title.length()));
    }

    private void printIndexInfo(PrintWriter pw) {
        ListMultimap<String, IndexInfo> infos = ArrayListMultimap.create();
        for (IndexInfo info : indexInfoService.getAllIndexInfo()) {
            infos.put(info.getType(), info);
        }

        pw.printf("Total number of indexes : %d%n", infos.size());
        for (String type : infos.keySet()){
            List<IndexInfo> typedInfo = infos.get(type);
            String title = String.format("%s(%d)", type, typedInfo.size());
            printTitle(pw, title);
            pw.println();
            for (IndexInfo info : typedInfo){
                printIndexInfo(pw, info);
            }
        }
    }

    private static void printIndexInfo(PrintWriter pw, IndexInfo info) {
        pw.println(info.getIndexPath());
        pw.printf("    Type                    : %s%n", info.getType());
        if (info.getAsyncLaneName() != null) {
            pw.printf("    Async                   : true%n");
            pw.printf("    Async lane name         : %s%n", info.getAsyncLaneName());
        }

        if (info.getIndexedUpToTime() > 0){
            pw.printf("    Last indexed up to      : %s%n", formatTime(info.getIndexedUpToTime()));
        }

        if (info.getLastUpdatedTime() > 0){
            pw.printf("    Last updated time       : %s%n", formatTime(info.getLastUpdatedTime()));
        }

        if (info.getSizeInBytes() >= 0){
            pw.printf("    Size                    : %s%n", IOUtils.humanReadableByteCount(info.getSizeInBytes()));
        }

        if (info.getEstimatedEntryCount() >= 0){
            pw.printf("    Estimated entry count   : %d%n", info.getEstimatedEntryCount());
        }

        if (info.hasIndexDefinitionChangedWithoutReindexing()) {
            pw.println("    Index definition changed without reindexing");
            String diff = info.getIndexDefinitionDiff();
            if (diff != null) {
                pw.println("    "+diff);
            }
        }
        pw.println();
    }

    private static String formatTime(long time){
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        return ISO8601.format(cal);
    }
}
