import groovy.transform.CompileStatic;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.segment.SegmentBlob;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.codehaus.groovy.tools.shell.CommandSupport;
import org.apache.jackrabbit.oak.console.ConsoleSession
import org.codehaus.groovy.tools.shell.Groovysh;

import java.util.concurrent.atomic.AtomicInteger;

@CompileStatic
class CountNodesCommand extends CommandSupport {
    static final String COMMAND_NAME = 'count-nodes';

    CountNodesCommand(Groovysh shell) {
        super(shell, COMMAND_NAME, 'countNodes');
    }

    @Override
    Object execute(List<String> args) {
        NodeStore store = getSession().getStore();
        NodeState rootState = store.getRoot();

        AtomicInteger count = new AtomicInteger(0);
        AtomicInteger binaries = new AtomicInteger(0);

        countNodes(rootState, "/", 1000000, 1000, count, binaries, true);

        io.out.println("Total nodes: " + count.get());
        io.out.println("Total binaries: " + binaries.get());

        return null;
    }

    void countNodes(NodeState n, String path, Integer flush, Long warnAt, AtomicInteger count, AtomicInteger binaries, boolean root) {
        if (root) {
            io.out.println("Counting nodes in tree " + path);
        }

        int cnt = count.incrementAndGet();
        if (cnt % flush == 0) {
            io.out.println("  " + cnt);
        }

        try {
            for (PropertyState prop : (Iterable<PropertyState>) n.getProperties()) {
                if (prop.getType() == Type.BINARY || prop.getType() == Type.BINARIES) {
                    for (Blob b : prop.getValue(Type.BINARIES)) {
                        binaries.incrementAndGet();
                        if (b instanceof SegmentBlob) {
                            if (!((SegmentBlob) b).isExternal()) {
                                b.length();
                            }
                        } else {
                            b.length();
                        }
                    }
                }
            }

            long kids = n.getChildNodeCount(warnAt);
            if (kids >= warnAt) {
                io.out.println(path + " has " + kids + " child nodes");
            }

            for (ChildNodeEntry child : (Iterable<ChildNodeEntry>) n.getChildNodeEntries()) {
                countNodes(child.getNodeState(), path + child.getName() + "/", flush, warnAt, count, binaries, false);
            }
        } catch (Exception e) {
            io.out.println("warning unable to read node " + path);
        }

        if (root) {
            io.out.println("Total nodes in tree " + path + ": " + cnt);
            io.out.println("Total binaries in tree " + path + ": " + binaries.get());
        }
    }

    ConsoleSession getSession() {
        return (ConsoleSession) variables.get("session");
    }
}