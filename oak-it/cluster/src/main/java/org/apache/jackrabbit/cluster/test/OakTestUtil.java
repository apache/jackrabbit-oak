package org.apache.jackrabbit.cluster.test;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.commit.JcrConflictHandler;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.name.NameValidatorProvider;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.plugins.version.VersionEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardIndexProvider;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import java.net.UnknownHostException;

/**
 * Created by Dominik Foerderreuther <df@adobe.com> on 29/02/16.
 */
public class OakTestUtil {

    public static Repository connect(MongoClient mongoClient, String dbname) throws UnknownHostException {
        DB db = mongoClient.getDB(dbname);

        DocumentNodeStore ns = new DocumentMK.Builder()
                .setMongoDB(db)
                //.setLogging(true)
                //.setAsyncDelay(100)
                .getNodeStore();

        //log.info(ns.getClusterInfo());
        return createOakRepository(ns);
    }

    public static Repository connect(MongoClient mongoClient, String dbname, int asyncDelay, int maxBackOffMillis) throws UnknownHostException {
        DB db = mongoClient.getDB(dbname);

        DocumentNodeStore ns = new DocumentMK.Builder()
                .setMongoDB(db)
                //.setLogging(true)
                .getNodeStore();

        ns.setAsyncDelay(asyncDelay);
        ns.setMaxBackOffMillis(maxBackOffMillis);

        return createOakRepository(ns);
    }

    public static Session session(Repository repository) throws RepositoryException {
        return repository.login(new SimpleCredentials("admin", "admin".toCharArray()), "test");
    }

    private static Repository createOakRepository(NodeStore nodeStore) {
        DefaultWhiteboard whiteboard = new DefaultWhiteboard();

        final WhiteboardIndexProvider indexProvider = new WhiteboardIndexProvider();

        final WhiteboardIndexEditorProvider indexEditorProvider = new WhiteboardIndexEditorProvider();
        indexProvider.start(whiteboard);
        indexEditorProvider.start(whiteboard);


        final Oak oak = new Oak(nodeStore)
                .with(new InitialContent())
                //.with(new ExtraSlingContent())

                .with(JcrConflictHandler.JCR_CONFLICT_HANDLER)
                .with(new EditorHook(new VersionEditorProvider()))

                .with(new OpenSecurityProvider())

                .with(new NameValidatorProvider())
                .with(new NamespaceEditorProvider())
                .with(new TypeEditorProvider())
                //.with(new RegistrationEditorProvider())
                .with(new ConflictValidatorProvider())

                // index stuff
                .with(indexProvider)
                .with(indexEditorProvider)
                .with("test")
                .withAsyncIndexing()
                .with(whiteboard);


//        if (commitRateLimiter != null) {
//            oak.with(commitRateLimiter);
//        }

        final ContentRepository contentRepository = oak.createContentRepository();
        return new RepositoryImpl(contentRepository, whiteboard, new OpenSecurityProvider(), 1000, null);
    }
}
