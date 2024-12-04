package org.apache.jackrabbit.oak.explorer;

import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.json.JsopBuilder.prettyPrint;
import static org.apache.jackrabbit.oak.json.JsopDiff.diffToJsop;

class DiffReport {

    private DiffReport() {
    }

    static String createPlainTextReport(String input, ExplorerBackend backend) {
        StringBuilder sb = new StringBuilder();
        if (input == null || input.trim().isEmpty()) {
            return "Usage <recordId> <recordId> [<path>]";
        }

        String[] tokens = input.trim().split(" ");
        if (tokens.length != 2 && tokens.length != 3) {
            return "Usage <recordId> <recordId> [<path>]";
        }
        NodeState node1;
        NodeState node2;
        try {
            node1 = backend.readNodeState(tokens[0]);
            node2 = backend.readNodeState(tokens[1]);
        } catch (IllegalArgumentException ex) {
            sb.append("Unknown argument: ");
            sb.append(input);
            sb.append(ReportUtils.newline);
            sb.append("Error: ");
            sb.append(ex.getMessage());
            sb.append(ReportUtils.newline);
            return sb.toString();
        }
        String path = "/";
        if (tokens.length == 3) {
            path = tokens[2];
        }

        for (String name : elements(path)) {
            node1 = node1.getChildNode(name);
            node2 = node2.getChildNode(name);
        }

        sb.append("SegmentNodeState diff ");
        sb.append(tokens[0]);
        sb.append(" vs ");
        sb.append(tokens[1]);
        sb.append(" at ");
        sb.append(path);
        sb.append(ReportUtils.newline);
        sb.append("--------");
        sb.append(ReportUtils.newline);
        sb.append(prettyPrint(diffToJsop(node1, node2)));
        return sb.toString();
    }
}
