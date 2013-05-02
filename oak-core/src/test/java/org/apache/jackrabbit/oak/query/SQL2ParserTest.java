package org.apache.jackrabbit.oak.query;

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;

import java.text.ParseException;

import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class SQL2ParserTest {

    private final NodeState types = new InitialContent().initialize(EMPTY_NODE)
            .getChildNode(JCR_SYSTEM).getChildNode(JCR_NODE_TYPES);
    private final SQL2Parser p = new SQL2Parser(types);

    @Test
    public void testIgnoreSqlComment() throws ParseException {
        p.parse("select * from [nt:unstructured] /* sql comment */");
        p.parse("select [jcr:path], [jcr:score], * from [nt:base] as a /* xpath: //* */");
    }

    @Test(expected = ParseException.class)
    public void testUnfinishedSqlComment() throws ParseException {
        p.parse("select [jcr:path], [jcr:score], * from [nt:base] as a /* xpath: //* ");
    }

}
