package org.neo4j.graphalgo.similarity;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.collection.MapUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SimilarityVectorAggregatorTest {

    @Test
    public void singleItem() {
        SimilarityVectorAggregator aggregator = new SimilarityVectorAggregator();

        Node node = mock(Node.class);
        when(node.getId()).thenReturn(1L);

        aggregator.next(node, 3.0);

        List<Map<String, Object>> expected = Collections.singletonList(
                MapUtil.map("id", 1L, "weight", 3.0)
        );

        assertThat(aggregator.result(), is(expected));
    }

    @Test
    public void multipleItems() {
        SimilarityVectorAggregator aggregator = new SimilarityVectorAggregator();

        Node node = mock(Node.class);
        when(node.getId()).thenReturn(1L, 2L, 3L);

        aggregator.next(node, 3.0);
        aggregator.next(node, 2.0);
        aggregator.next(node, 1.0);

        List<Map<String, Object>> expected = Arrays.asList(
                MapUtil.map("id", 1L, "weight", 3.0),
                MapUtil.map("id", 2L, "weight", 2.0),
                MapUtil.map("id", 3L, "weight", 1.0)
        );

        assertThat(aggregator.result(), is(expected));
    }

}