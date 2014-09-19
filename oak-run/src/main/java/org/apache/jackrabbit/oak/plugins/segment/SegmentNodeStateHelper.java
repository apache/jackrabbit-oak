package org.apache.jackrabbit.oak.plugins.segment;

public class SegmentNodeStateHelper {

    private SegmentNodeStateHelper() {

    }

    public static RecordId getTemplateId(SegmentNodeState s) {
        return s.getTemplateId();
    }

}
