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
                { "/", "foo", "bar", "", "/|{\"foo\":\"bar\"}", "/|{\"foo\":\"bar\"}" },
                { "/test", "foo", "bar", "", "/1test|{\"foo\":\"bar\"}", "/test|{\"foo\":\"bar\"}" },
                { "/dir/asset", "key", "value", "", "/1dir/1asset|{\"key\":\"value\"}",
                        "/dir/asset|{\"key\":\"value\"}" },
                { "/content/dam/jcr:content", "foo", "bar", "jcr:content",
                        "/1content/1dam/0jcr:content|{\"foo\":\"bar\"}",
                        "/content/dam/jcr:content|{\"foo\":\"bar\"}" },
                { "/content/dam/jcr:content/test", "foo", "bar", "jcr:content,dam",
                        "/1content/0dam/0jcr:content/1test|{\"foo\":\"bar\"}",
                        "/content/dam/jcr:content/test|{\"foo\":\"bar\"}" },
                { "/1/2/3/4/5/6/7/8/9/10/11/12", "12levels", "testcase", "jcr:content,dam",
                        "/11/12/13/14/15/16/17/18/19/110/111/112|{\"12levels\":\"testcase\"}",
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
        assertEquals(expectedDeserialized, deserialized);
    }
}
