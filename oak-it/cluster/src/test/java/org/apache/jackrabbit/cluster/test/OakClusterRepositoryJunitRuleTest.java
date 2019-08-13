package org.apache.jackrabbit.cluster.test;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import javax.jcr.Repository;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by Dominik Foerderreuther <df@adobe.com> on 03/03/16.
 */
public class OakClusterRepositoryJunitRuleTest {

    @Rule
    public OakClusterRepository oakClusterRepository = new OakClusterRepository();

    @Test
    public void shouldCreateClusterOfThree() throws Exception {
        Repository one = oakClusterRepository.repository();
        Repository two = oakClusterRepository.repository();
        Repository three = oakClusterRepository.repository();

        assertThat(Sets.newHashSet(clusterIdOf(one), clusterIdOf(two), clusterIdOf(three)).size(), is(3));
    }

    private int clusterIdOf(Repository repository) {
        ContentRepositoryImpl contentRepository = Whitebox.getInternalState(repository, "contentRepository");
        DocumentNodeStore documentNodeStore = Whitebox.getInternalState(contentRepository, "nodeStore");
        return documentNodeStore.getClusterId();
    }

}
