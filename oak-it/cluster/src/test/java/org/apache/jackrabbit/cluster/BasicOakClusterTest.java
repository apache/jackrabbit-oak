package org.apache.jackrabbit.cluster;

import org.apache.jackrabbit.cluster.test.OakClusterRepository;
import org.apache.jackrabbit.cluster.test.OakTestUtil;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.powermock.reflect.Whitebox;

import javax.jcr.InvalidItemStateException;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;

import static java.lang.Thread.sleep;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * Created by Dominik Foerderreuther <df@adobe.com> on 29/02/16.
 */
public class BasicOakClusterTest {

    public static final Logger log = Logger.getLogger(BasicOakClusterTest.class);

    @Rule
    public OakClusterRepository oakClusterRepository = new OakClusterRepository();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void isInClusterMode() throws Exception {
        assertThat(clusterIdOf(oakClusterRepository.repository()), is(not(clusterIdOf(oakClusterRepository.repository()))));
    }

    @Test
    public void canLoginOnBothRepositories() throws Exception {
        Repository repositoryOne = oakClusterRepository.repository();
        Repository repositoryTwo = oakClusterRepository.repository();

        Session sessionOne = OakTestUtil.session(repositoryOne);
        assertThat(sessionOne.isLive(), is(true));

        Session sessionTwo = OakTestUtil.session(repositoryTwo);
        assertThat(sessionTwo.isLive(), is(true));
    }

    @Test
    public void save_readWithinSameSession_success() throws Exception {
        // given
        Session session = OakTestUtil.session(oakClusterRepository.repository());
        final String name = "test2";

        session.getRootNode().addNode(name);

        // when
        session.save();
        final Node readNode = session.getRootNode().getNode(name);

        // then
        assertThat(readNode, notNullValue());
        assertThat(readNode.getName(), is(name));

        session.logout();

    }

    @Test
    public void save_readAcrossCluster_success() throws Exception {
        // given
        Repository repositoryOne = oakClusterRepository.repository();
        Repository repositoryTwo = oakClusterRepository.repository();
        Session sessionOne = OakTestUtil.session(repositoryOne);

        final String name = "test2";

        // when
        sessionOne.getRootNode().addNode(name);
        sessionOne.save();
        sessionOne.logout();

        sleep(1000);

        Session sessionTwo = OakTestUtil.session(repositoryTwo);
        final Node readNode = sessionTwo.getRootNode().getNode(name);

        // then
        assertThat(readNode, notNullValue());
        assertThat(readNode.getName(), is(name));
        sessionTwo.logout();

    }

    @Test
    public void saveOnBothReporitories_refreshAndReadOnSecond_success() throws Exception {
        // given
        Session sessionTwo = OakTestUtil.session(oakClusterRepository.repository());
        Session sessionOne = OakTestUtil.session(oakClusterRepository.repository());

        sessionOne.getRootNode().addNode("etc").addNode("test").addNode("a");
        sessionOne.save();

        sessionTwo.getRootNode().addNode("etc").addNode("test").addNode("b");
        sessionTwo.save();

        sleep(1000);

        // when
        sessionOne.refresh(true);

        final Node a = sessionOne.getRootNode().getNode("etc/test/a");
        final Node b = sessionOne.getRootNode().getNode("etc/test/b");

        // then
        assertThat(a, notNullValue());
        assertThat(b, notNullValue());

        assertThat(a.getName(), is("a"));
        assertThat(b.getName(), is("b"));

    }

    @Test
    public void saveSameAttributeWithSameValueOnBothRepositories_success() throws Exception {
        // given
        Session sessionOne = OakTestUtil.session(oakClusterRepository.repository());
        Session sessionTwo = OakTestUtil.session(oakClusterRepository.repository());

        // when
        final Node nodeOne = sessionOne.getRootNode().addNode("etc").addNode("a");
        nodeOne.setProperty("test", 1);
        sessionOne.save();

        sleep(10000);

        final Node nodeTwo = sessionTwo.getRootNode().addNode("etc").addNode("a");
        nodeTwo.setProperty("test", 1);
        sessionTwo.save();

        // then

        final Node a = sessionOne.getRootNode().getNode("etc/a");
        final Node b = sessionOne.getRootNode().getNode("etc/a");

        // then
        assertThat(a, notNullValue());
        assertThat(b, notNullValue());

        assertThat(a.getProperty("test").getLong(), is(1l));
        assertThat(b.getProperty("test").getLong(), is(1l));
    }

    @Test
    public void saveSameAttributeOnBothRepositories_fail() throws Exception {
        // given
        Session sessionOne = OakTestUtil.session(oakClusterRepository.repository());
        Session sessionTwo = OakTestUtil.session(oakClusterRepository.repository());

        // when
        final Node nodeOne = sessionOne.getRootNode().addNode("etc").addNode("a");
        nodeOne.setProperty("test", 1);
        sessionOne.save();

        final Node nodeTwo = sessionTwo.getRootNode().addNode("etc").addNode("a");
        nodeTwo.setProperty("test", 2);

        // then
        expectedException.expect(InvalidItemStateException.class);
        expectedException.expectMessage("OakState0001: Unresolved conflicts in /etc/a");
        sessionTwo.save();

    }

    @Test
    public void saveNodesOnExistingParent_success() throws Exception {
        // given
        Session sessionOne = OakTestUtil.session(oakClusterRepository.repository());
        Session sessionTwo = OakTestUtil.session(oakClusterRepository.repository());

        final String existing = "existing";
        sessionOne.getRootNode().addNode(existing);
        sessionOne.save();

        sleep(1000);

        sessionTwo.refresh(false);

        // when
        final Node nodeOne = sessionOne.getRootNode().getNode(existing).addNode("a");
        sessionOne.save();

        final Node nodeTwo = sessionTwo.getRootNode().getNode(existing).addNode("b");
        sessionTwo.save();

        sleep(1000);

        // then
        sessionOne.refresh(false);
        assertThat(sessionOne.getRootNode().getNode(existing).getNode("a"), notNullValue());
        assertThat(sessionOne.getRootNode().getNode(existing).getNode("b"), notNullValue());
    }

    private int clusterIdOf(Repository repository) {
        ContentRepositoryImpl contentRepository = Whitebox.getInternalState(repository, "contentRepository");
        DocumentNodeStore documentNodeStore = Whitebox.getInternalState(contentRepository, "nodeStore");
        return documentNodeStore.getClusterId();
    }

}