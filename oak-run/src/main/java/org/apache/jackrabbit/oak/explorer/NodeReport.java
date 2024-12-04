package org.apache.jackrabbit.oak.explorer;

import j2html.tags.ContainerTag;
import j2html.tags.DomContent;
import j2html.tags.specialized.*;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static j2html.TagCreator.*;
import static javax.jcr.PropertyType.BINARY;
import static javax.jcr.PropertyType.STRING;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.jackrabbit.guava.common.escape.Escapers.builder;

class NodeReport {

    private static final int MAX_CHAR_DISPLAY = Integer.getInteger("max.char.display", 60);

    private NodeReport() {
    }

    public static String createHtmlTextReport(NodeStoreTree.NamePathModel model, ExplorerBackend backend) {
        BodyTag body = body();
        NodeState state = model.getState();
        body.with(createPathTag(model));

        String tarFile = "";
        if (backend.isPersisted(state)) {
            tarFile = backend.getFile(state);
            body.with(createRecordTag(backend, state, tarFile));
            body.with(createTemplateIdTag(backend, state, tarFile));
        }
        body.with(createSizeTag(model));
        body.with(createPropertiesTag(backend, state, tarFile));
        body.with(createChildNodesTag(backend, state, tarFile));
        body.with(createFileReaderIndexTag(model, backend));

        return html(body).render();
    }

    private static DivTag createPathTag(NodeStoreTree.NamePathModel model) {
        return div(b("Path: ")).withText(model.getPath());
    }

    private static DivTag createFileReaderIndexTag(NodeStoreTree.NamePathModel model, ExplorerBackend backend) {
        DivTag tag = div();
        if ("/".equals(model.getPath())) {
            tag.withText("File Reader Index ");
            for (String path : backend.getTarFiles()) {
                tag.with(createTarTag(path));
            }
            tag.withText("----------");
        }
        return div();
    }


    private static DivTag createRecordTag(ExplorerBackend backend, NodeState state, String tarFile) {
        DivTag tag = div(b("Record "));
        tag.withText(backend.getRecordId(state));
        if (tarFile != null) {
            tag.withText(" in ");
            tag.with(createTarTag(tarFile));
        }
        return tag;
    }

    private static DomContent createTemplateIdTag(ExplorerBackend backend, NodeState state, String tarFile) {
        DivTag tag = div(b("TemplateId "));
        String templateId = backend.getTemplateRecordId(state);
        String templateFile = backend.getTemplateFile(state);

        tag.withText(templateId);
        if (templateFile != null && !templateFile.equals(tarFile)) {
            tag.withText(" in ");
            tag.with(createTarTag(templateFile));
        }
        return tag;
    }

    private static DomContent createSizeTag(NodeStoreTree.NamePathModel model) {
        DivTag tag = div(b("Size"));
        tag.withText("  direct: " + byteCountToDisplaySize(model.getSize()[0]));
        tag.withText(";  linked: " + byteCountToDisplaySize(model.getSize()[1]));
        return tag;
    }

    private static DomContent createPropertiesTag(ExplorerBackend backend, NodeState state, String tarFile) {
        DivTag tag = div(b("Properties")).withText(" (count: " + state.getPropertyCount() + ")");

        Map<String, DomContent> propLines = new TreeMap<>();
        for (PropertyState ps : state.getProperties()) {
            DomContent propertyTag = createPropertyTag(backend, tarFile, ps);
            propLines.put(ps.getName(), propertyTag);
        }
        UlTag ul = ul();
        for (DomContent l : propLines.values()) {
            ul.with((l));
        }
        tag.with(ul);
        return tag;
    }

    private static DomContent createPropertyTag(ExplorerBackend backend, String tarFile, PropertyState ps) {
        LiTag tag = li(ps.getName() + " = {" + ps.getType() + "} ");
        if (ps.getType().isArray()) {
            tag.with(getArrayPropertyRepresentationTag(backend, tarFile, ps));
        } else {
            tag.withText(getScalarPropertyRepresentationText(ps, 0, tarFile, backend));
        }
        printPropertyState(backend, ps, tarFile, tag);
        return tag;
    }

    private static DomContent getArrayPropertyRepresentationTag(ExplorerBackend backend, String tarFile, PropertyState ps) {
        StringBuilder l = new StringBuilder();
        l.append("<div>");

        int count = ps.count();
        l.append("(count ").append(count).append(") [");

        String separator = ", ";
        int max = 50;
        if (ps.getType() == Type.BINARIES) {
            separator = "<br>      ";
            max = Integer.MAX_VALUE;
            l.append(separator);
        }
        for (int i = 0; i < Math.min(count, max); i++) {
            if (i > 0) {
                l.append(separator);
            }
            l.append(getScalarPropertyRepresentationText(ps, i, tarFile, backend));
        }
        if (count > max) {
            l.append(", ... (").append(count).append(" values)");
        }
        if (ps.getType() == Type.BINARIES) {
            l.append(separator);
        }
        l.append("]");
        l.append("</div>");

        return rawHtml(l.toString());
    }

    private static DomContent createChildNodesTag(ExplorerBackend backend, NodeState state, String tarFile) {
        DivTag div = div();

        div.with(b("Child nodes")).withText(" (count: ").withText(String.valueOf(state.getChildNodeCount(Long.MAX_VALUE))).withText(")");
        Map<String, DomContent> childLines = new TreeMap<>();

        for (ChildNodeEntry ce : state.getChildNodeEntries()) {
            DomContent tag = createChildNodeTag(backend, tarFile, ce);
            childLines.put(ce.getName(), tag);
        }

        UlTag ul = ul();
        for (DomContent value : childLines.values()) {
            ul.with(value);
        }
        div.with(ul);
        return div;
    }

    private static DomContent createChildNodeTag(ExplorerBackend backend, String tarFile, ChildNodeEntry ce) {
        LiTag tag = li(ce.getName());
        NodeState c = ce.getNodeState();
        printNodeState(backend, c, tarFile, tag);
        return tag;
    }

    private static String getScalarPropertyRepresentationText(PropertyState ps, int index, String tarFile, ExplorerBackend backend) {
        if (ps.getType().tag() == BINARY) {
            Blob b = ps.getValue(Type.BINARY, index);
            String info = "<";
            info += b.getClass().getSimpleName() + ";";
            info += "ref:" + safeGetReference(b) + ";";
            info += "id:" + b.getContentIdentity() + ";";
            info += safeGetLength(b) + ">";

            UlTag ulTag = ul();
            for (Map.Entry<UUID, String> e : backend.getBulkSegmentIds(b).entrySet()) {
                LiTag liTag = li("Bulk Segment Id " + e.getKey());
                if (e.getValue() != null && !e.getValue().equals(tarFile)) {
                    liTag.withText(" in " + e.getValue());
                }
                ulTag.with(liTag);
            }
            return info;
        } else if (ps.getType().tag() == STRING) {
            return displayString(ps.getValue(Type.STRING, index));
        } else {
            return ps.getValue(Type.STRING, index);
        }
    }

    private static String safeGetReference(Blob b) {
        try {
            return b.getReference();
        } catch (IllegalStateException e) {
            // missing BlobStore probably
        }
        return "[BlobStore not available]";
    }

    private static String safeGetLength(Blob b) {
        try {
            return byteCountToDisplaySize(b.length());
        } catch (IllegalStateException e) {
            // missing BlobStore probably
        }
        return "[BlobStore not available]";
    }

    public static String displayString(String value) {
        if (MAX_CHAR_DISPLAY > 0 && value.length() > MAX_CHAR_DISPLAY) {
            value = value.substring(0, MAX_CHAR_DISPLAY) + "... ("
                    + value.length() + " chars)";
        }
        String escaped = builder().setSafeRange(' ', '~')
                .addEscape('"', "\\\"").addEscape('\\', "\\\\").build()
                .escape(value);
        return '"' + escaped + '"';
    }

    private static void printPropertyState(ExplorerBackend store, PropertyState state, String parentFile, ContainerTag<?> tag) {
        if (store.isPersisted(state)) {
            printRecordId(store.getRecordId(state), store.getFile(state), parentFile, tag);
        } else {
            printSimpleClassName(state, tag);
        }
    }

    private static void printNodeState(ExplorerBackend store, NodeState state, String parentTarFile, ContainerTag<?> tag) {
        if (store.isPersisted(state)) {
            printRecordId(store.getRecordId(state), store.getFile(state), parentTarFile, tag);
        } else {
            printSimpleClassName(state, tag);
        }
    }

    private static void printRecordId(String recordId, String file, String parentFile, ContainerTag<?> tag) {
        tag.withText(" (").withText(recordId);

        if (file != null && !file.equals(parentFile)) {
            tag.withText(" in ").with(createTarTag(file));
        }
        tag.withText(")");
    }

    private static void printSimpleClassName(Object o, ContainerTag<?> tag) {
        tag.withText(" (").withText(o.getClass().getSimpleName()).withText(")");
    }

    private static ATag createTarTag(String tarFileName) {
        return a(tarFileName).withHref("tar://" + tarFileName);
    }
}
