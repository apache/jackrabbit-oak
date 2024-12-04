package org.apache.jackrabbit.oak.explorer;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.*;

import static javax.jcr.PropertyType.BINARY;
import static javax.jcr.PropertyType.STRING;
import static org.apache.jackrabbit.oak.explorer.NodeReport.displayString;

class ReportUtils {

    public static final String newline = "\n";

    private ReportUtils() {
    }

    static void printGcRoots(StringBuilder sb, Map<UUID, Set<Map.Entry<UUID, String>>> links, UUID uuid, String space, String inc) {

        Set<Map.Entry<UUID, String>> roots = links.remove(uuid);

        if (roots == null || roots.isEmpty()) {
            return;
        }

        // TODO is sorting by file name needed?
        for (Map.Entry<UUID, String> r : roots) {
            sb.append(space).append(r.getKey()).append("[").append(r.getValue()).append("]").append(newline);
            printGcRoots(sb, links, r.getKey(), space + inc, inc);
        }
    }

    public static void printPaths(List<String> paths, StringBuilder sb) {
        if (paths.isEmpty()) {
            return;
        }

        sb.append("Repository content references:").append(newline);

        for (String p : paths) {
            sb.append(p).append(newline);
        }
    }

    public static void filterNodeStates(Set<UUID> uuids, List<String> paths, NodeState state, String path, ExplorerBackend store) {
        Set<String> localPaths = new TreeSet<>();
        for (PropertyState ps : state.getProperties()) {
            if (store.isPersisted(ps)) {
                String recordId = store.getRecordId(ps);
                UUID id = store.getSegmentId(ps);
                if (uuids.contains(id)) {
                    if (ps.getType().tag() == STRING) {
                        String val = "";
                        if (ps.count() > 0) {
                            // only shows the first value, do we need more?
                            val = displayString(ps.getValue(Type.STRING, 0));
                        }
                        localPaths.add(path + ps.getName() + " = " + val
                                + " [SegmentPropertyState<" + ps.getType()
                                + ">@" + recordId + "]");
                    } else {
                        localPaths.add(path + ps + " [SegmentPropertyState<"
                                + ps.getType() + ">@" + recordId + "]");
                    }

                }
                if (ps.getType().tag() == BINARY) {
                    // look for extra segment references
                    for (int i = 0; i < ps.count(); i++) {
                        Blob b = ps.getValue(Type.BINARY, i);
                        for (Map.Entry<UUID, String> e : store.getBulkSegmentIds(b).entrySet()) {
                            if (!e.getKey().equals(id) && uuids.contains(e.getKey())) {
                                localPaths.add(path + ps
                                        + " [SegmentPropertyState<"
                                        + ps.getType() + ">@" + recordId + "]");
                            }
                        }
                    }
                }
            }
        }

        String stateId = store.getRecordId(state);
        if (uuids.contains(store.getSegmentId(state))) {
            localPaths.add(path + " [SegmentNodeState@" + stateId + "]");
        }

        String templateId = store.getTemplateRecordId(state);
        if (uuids.contains(store.getTemplateSegmentId(state))) {
            localPaths.add(path + "[Template@" + templateId + "]");
        }
        paths.addAll(localPaths);
        for (ChildNodeEntry ce : state.getChildNodeEntries()) {
            filterNodeStates(uuids, paths, ce.getNodeState(), path + ce.getName() + "/", store);
        }
    }
}
