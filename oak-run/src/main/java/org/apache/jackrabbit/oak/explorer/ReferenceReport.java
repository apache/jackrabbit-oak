package org.apache.jackrabbit.oak.explorer;

import java.io.IOException;
import java.util.*;

import static org.apache.jackrabbit.oak.explorer.ReportUtils.newline;

class ReferenceReport {

    static String createPlainReport(String sid, ExplorerBackend backend, Map<String, Set<UUID>> index) {
        UUID id;
        try {
            id = UUID.fromString(sid.trim());
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("References to segment ").append(id);
        sb.append(newline);
        for (Map.Entry<String, Set<UUID>> e : index.entrySet()) {
            if (e.getValue().contains(id)) {
                sb.append("Tar file: ").append(e.getKey());
                sb.append(newline);
                break;
            }
        }

        List<String> paths = new ArrayList<>();
        ReportUtils.filterNodeStates(Set.of(id), paths, backend.getHead(), "/", backend);
        ReportUtils.printPaths(paths, sb);

        Map<UUID, Set<Map.Entry<UUID, String>>> links = new HashMap<>();
        try {
            backend.getGcRoots(id, links);
        } catch (IOException e) {
            sb.append(newline);
            sb.append(e.getMessage());
        }
        if (!links.isEmpty()) {
            sb.append("Segment GC roots:");
            sb.append(newline);
            ReportUtils.printGcRoots(sb, links, id, "  ", "  ");
        }
        return sb.toString();
    }

}
