package org.apache.jackrabbit.oak.explorer;

import java.io.IOException;
import java.util.*;

import static org.apache.jackrabbit.guava.common.collect.Sets.intersection;

class TarReport {

    private TarReport() {
    }

    public static String createPlainTextReport(String file, ExplorerBackend backend, Map<String, Set<UUID>> index) {
        StringBuilder sb = new StringBuilder();

        Set<UUID> uuids = new HashSet<>();
        for (Map.Entry<String, Set<UUID>> e : index.entrySet()) {
            if (e.getKey().endsWith(file)) {
                sb.append("SegmentNodeState references to ").append(e.getKey());
                sb.append(ReportUtils.newline);
                uuids = e.getValue();
                break;
            }
        }

        Set<UUID> inMem = intersection(backend.getReferencedSegmentIds(), uuids);
        if (!inMem.isEmpty()) {
            sb.append("In Memory segment references: ");
            sb.append(ReportUtils.newline);
            sb.append(inMem);
            sb.append(ReportUtils.newline);
        }

        List<String> paths = new ArrayList<>();
        ReportUtils.filterNodeStates(uuids, paths, backend.getHead(), "/", backend);
        ReportUtils.printPaths(paths, sb);

        sb.append(ReportUtils.newline);
        try {
            Map<UUID, Set<UUID>> graph = backend.getTarGraph(file);
            sb.append("Tar graph:").append(ReportUtils.newline);
            for (Map.Entry<UUID, Set<UUID>> entry : graph.entrySet()) {
                sb.append(entry.getKey()).append('=').append(entry.getValue())
                        .append(ReportUtils.newline);
            }
            sb.append(ReportUtils.newline);
        } catch (IOException e) {
            sb.append("Error getting tar graph:").append(e).append(ReportUtils.newline);
        }
        return sb.toString();
    }

}
