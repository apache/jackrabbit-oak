package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.*;

import static com.google.common.collect.ImmutableList.copyOf;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class NodeStateEntryWriterSerializeTest {
    private BlobStore blobStore = new MemoryBlobStore();
    private String inputPath;
    private String propKey;
    private String propVal;
    private String preferredStr;
    private String expectedSerialized;
    private String expectedDeserialized;

    public NodeStateEntryWriterSerializeTest(String inputPath, String propKey, String propVal, String preferredStr,
                                             String expectedSerialized, String expectedDeserialized) {
        this.inputPath = inputPath;
        this.propKey = propKey;
        this.propVal = propVal;
        this.expectedSerialized = expectedSerialized;
        this.preferredStr = preferredStr;
        this.expectedDeserialized = expectedDeserialized;
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                //inputPath propKey propVal preferredStr expectedSerialized expectedDeserialized
                { "/", "foo", "bar", "", "000/|{\"foo\":\"bar\"}", "/|{\"foo\":\"bar\"}" },
                { "/test", "foo", "bar", "", "001/1]test|{\"foo\":\"bar\"}", "/test|{\"foo\":\"bar\"}" },
                { "/dir/asset", "key", "value", "", "002/1]dir/1]asset|{\"key\":\"value\"}",
                        "/dir/asset|{\"key\":\"value\"}" },
                { "/content/dam/jcr:content", "foo", "bar", "jcr:content",
                        "003/1]content/1]dam/0]jcr:content|{\"foo\":\"bar\"}",
                        "/content/dam/jcr:content|{\"foo\":\"bar\"}" },
                { "/content/dam/jcr:content/test", "foo", "bar", "jcr:content,dam",
                        "004/1]content/0]dam/0]jcr:content/1]test|{\"foo\":\"bar\"}",
                        "/content/dam/jcr:content/test|{\"foo\":\"bar\"}" },
                { "/1/2/3/4/5/6/7/8/9/10/11/12", "12levels", "testcase", "jcr:content,dam",
                        "012/1]1/1]2/1]3/1]4/1]5/1]6/1]7/1]8/1]9/1]10/1]11/1]12|{\"12levels\":\"testcase\"}",
                        "/1/2/3/4/5/6/7/8/9/10/11/12|{\"12levels\":\"testcase\"}" },
        });
    }

    @Test
    public void test() {
        NodeStateEntryWriter nw = new NodeStateEntryWriter(blobStore);
        NodeBuilder b1 = EMPTY_NODE.builder();
        b1.setProperty(propKey, propVal);

        NodeStateEntry e1 = new NodeStateEntry.NodeStateEntryBuilder(b1.getNodeState(), inputPath).build();

        String json = nw.asJson(e1.getNodeState());
        List<String> pathElements = copyOf(elements(e1.getPath()));
        Set<String> preferred = new HashSet<>(Arrays.asList(preferredStr.split(",")));

        String serialized = nw.serialize(pathElements, json, preferred);
        assertEquals(expectedSerialized, serialized);

        String deserialized = nw.deserialize(serialized);
        System.out.println(deserialized);
        assertEquals(expectedDeserialized, deserialized);
    }
}
