package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.apache.jackrabbit.oak.plugins.document.NodeDocument;

import java.util.HashMap;

class EndingStatistics {
    public int count = 0;
    public long size = 0;

    @Override
    public String toString() {
        return "{" +
                "count=" + count +
                ", size=" + size +
                '}';
    }
}

public class EndingsTracker {
    final HashMap<String, EndingStatistics> endings = new HashMap<>();

    public void trackDocument(NodeDocument next) {
        var id = getPath(next);
        var index = id.lastIndexOf("/jcr:content");
        if (index != -1) {
            var ending = id.substring(index);
            var predictedTagsIndex  = ending.indexOf("/metadata/predictedTags");
            if (predictedTagsIndex != -1)  {
                ending = ending.substring(0, predictedTagsIndex + "/metadata/predictedTags".length()) + "/*";
            }
            int docSize = (int) next.get(NodeDocumentCodec.SIZE_FIELD);
            var stats = endings.computeIfAbsent(ending, k -> new EndingStatistics());
            stats.count++;
            stats.size += docSize;
        }
    }

    private String getPath(NodeDocument next) {
        String p = (String) next.get(NodeDocument.PATH);
        if (p != null) {
            return p;
        }
        return next.getId();
    }
}
