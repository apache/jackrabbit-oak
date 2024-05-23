import org.apache.jackrabbit.oak.spi.commit.CommitInfo
import org.apache.jackrabbit.oak.spi.commit.EmptyHook
import org.apache.jackrabbit.oak.commons.PathUtils
import org.apache.jackrabbit.oak.console.ConsoleSession
import org.apache.jackrabbit.oak.spi.state.NodeStore
import org.codehaus.groovy.tools.shell.CommandSupport
import org.codehaus.groovy.tools.shell.Groovysh
import groovy.transform.CompileStatic

@CompileStatic
class RemoveNodeCommand extends CommandSupport {
    static final String COMMAND_NAME = 'remove-node'

    RemoveNodeCommand(Groovysh shell) {
        super(shell, COMMAND_NAME, "rmNode")
    }

    @Override
    Object execute(List<String> args) {
        if (args.isEmpty()) {
            throw new IllegalArgumentException("Usage: rmNode <path>")
        }
        
        String path = args[0]
        ConsoleSession session = getSession()
        NodeStore nodeStore = session.getStore()
        boolean result = removeNode(nodeStore, path)
        
        if (result) {
            io.out.println("Node at path '${path}' removed successfully.")
        } else {
            io.out.println("Node at path '${path}' does not exist or could not be removed.")
        }
        
        return null
    }

    private boolean removeNode(NodeStore nodeStore, String path) {
        def rootBuilder = nodeStore.root.builder()
        def targetBuilder = rootBuilder
        
        PathUtils.elements(path).each { element ->
            targetBuilder = targetBuilder.getChildNode(element)
        }
        
        if (targetBuilder.exists()) {
            targetBuilder.remove()
            nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY)
            return true
        } else {
            return false
        }
    }

    private ConsoleSession getSession() {
        return (ConsoleSession) variables.get("session");
    }
}