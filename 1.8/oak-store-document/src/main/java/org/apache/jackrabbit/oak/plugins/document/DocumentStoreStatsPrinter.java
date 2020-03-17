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
package org.apache.jackrabbit.oak.plugins.document;

import java.io.PrintWriter;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.base.Strings;

import org.apache.felix.inventory.Format;
import org.apache.felix.inventory.InventoryPrinter;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

/**
 * Inventory printer for {@link DocumentStore#getStats()}.
 */
@Component(property = {
        "felix.inventory.printer.name=oak-document-store-stats",
        "felix.inventory.printer.title=Oak DocumentStore Statistics",
        "felix.inventory.printer.format=TEXT"
})
public class DocumentStoreStatsPrinter implements InventoryPrinter {

    @Reference
    DocumentNodeStore nodeStore;

    @Override
    public void print(PrintWriter pw, Format format, boolean isZip) {
        if (format != Format.TEXT) {
            return;
        }
        DocumentStore store = nodeStore.getDocumentStore();
        printTitle(pw, "DocumentStore metadata");
        print(pw, store.getMetadata());
        pw.println();
        printTitle(pw, "DocumentStore statistics");
        print(pw, store.getStats());
        pw.flush();
    }

    private void printTitle(PrintWriter pw, String title) {
        pw.println(title);
        pw.println(Strings.repeat("=", title.length()));
    }

    private void print(PrintWriter pw, Map<String, String> data) {
        SortedMap<String, String> sortedData = new TreeMap<>(data);
        sortedData.forEach((k, v) -> pw.println(k + "=" + v));
    }
}
