package org.neo4j.graphalgo.similarity;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLog;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class SimilarityExporterTest {
    @Rule
    public final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    private static final String RELATIONSHIP_TYPE = "SIMILAR";
    private static final String PROPERTY_NAME = "score";

    @Test
    public void createNothing() {
        GraphDatabaseAPI api = DB.getGraphDatabaseAPI();
        createNodes(api, 2);

        SequentialSimilarityExporter exporter = new SequentialSimilarityExporter(api, NullLog.getInstance(), RELATIONSHIP_TYPE, PROPERTY_NAME);

        Stream<SimilarityResult> similarityPairs = Stream.empty();

        int batches = exporter.export(similarityPairs, 1);
        assertEquals(0, batches);

        try (Transaction tx = api.beginTx()) {
            List<SimilarityRelationship> allRelationships = getSimilarityRelationships(api);
            assertThat(allRelationships, hasSize(0));
        }
    }

    @Test
    public void createOneRelationship() {
        GraphDatabaseAPI api = DB.getGraphDatabaseAPI();
        createNodes(api, 2);

        SequentialSimilarityExporter exporter = new SequentialSimilarityExporter(api, NullLog.getInstance(), RELATIONSHIP_TYPE, PROPERTY_NAME);

        Stream<SimilarityResult> similarityPairs = Stream.of(new SimilarityResult(0, 1, -1, -1, -1, 0.5));

        int batches = exporter.export(similarityPairs, 1);
        assertEquals(1, batches);

        try (Transaction tx = api.beginTx()) {
            List<SimilarityRelationship> allRelationships = getSimilarityRelationships(api);
            assertThat(allRelationships, hasSize(1));
            assertThat(allRelationships, hasItems(new SimilarityRelationship(0, 1, 0.5)));
        }
    }

    @Test
    public void multipleBatches() {
        GraphDatabaseAPI api = DB.getGraphDatabaseAPI();
        createNodes(api, 4);

        SequentialSimilarityExporter exporter = new SequentialSimilarityExporter(api, NullLog.getInstance(), RELATIONSHIP_TYPE, PROPERTY_NAME);

        Stream<SimilarityResult> similarityPairs = Stream.of(
                new SimilarityResult(0, 1, -1, -1, -1, 0.5),
                new SimilarityResult(2, 3, -1, -1, -1, 0.7)
        );

        int batches = exporter.export(similarityPairs, 1);
        assertEquals(2, batches);

        try (Transaction tx = api.beginTx()) {
            List<SimilarityRelationship> allRelationships = getSimilarityRelationships(api);

            assertThat(allRelationships, hasSize(2));
            assertThat(allRelationships, hasItems(new SimilarityRelationship(0, 1, 0.5)));
            assertThat(allRelationships, hasItems(new SimilarityRelationship(2, 3, 0.7)));
        }
    }

    @Test
    public void smallerThanBatchSize() {
        GraphDatabaseAPI api = DB.getGraphDatabaseAPI();
        createNodes(api, 5);

        SequentialSimilarityExporter exporter = new SequentialSimilarityExporter(api, NullLog.getInstance(), RELATIONSHIP_TYPE, PROPERTY_NAME);

        Stream<SimilarityResult> similarityPairs = Stream.of(
                new SimilarityResult(0, 1, -1, -1, -1, 0.5),
                new SimilarityResult(2, 3, -1, -1, -1, 0.7),
                new SimilarityResult(3, 4, -1, -1, -1, 0.7)
        );

        int batches = exporter.export(similarityPairs, 10);
        assertEquals(1, batches);

        try (Transaction tx = api.beginTx()) {
            List<SimilarityRelationship> allRelationships = getSimilarityRelationships(api);

            assertThat(allRelationships, hasSize(3));
            assertThat(allRelationships, hasItems(new SimilarityRelationship(0, 1, 0.5)));
            assertThat(allRelationships, hasItems(new SimilarityRelationship(2, 3, 0.7)));
            assertThat(allRelationships, hasItems(new SimilarityRelationship(3, 4, 0.7)));
        }
    }

    private List<SimilarityRelationship> getSimilarityRelationships(GraphDatabaseAPI api) {
        return api.getAllRelationships().stream()
                .map(relationship -> new SimilarityRelationship(relationship.getStartNodeId(), relationship.getEndNodeId(), (double)relationship.getProperty(PROPERTY_NAME)))
                .collect(Collectors.toList());
    }

    static class SimilarityRelationship {
        private final long startNodeId;
        private final long endNodeId;
        private final double property;

        SimilarityRelationship(long startNodeId, long endNodeId, double property) {
            this.startNodeId = startNodeId;
            this.endNodeId = endNodeId;
            this.property = property;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SimilarityRelationship that = (SimilarityRelationship) o;
            return startNodeId == that.startNodeId &&
                    endNodeId == that.endNodeId &&
                    Double.compare(that.property, property) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(startNodeId, endNodeId, property);
        }
    }

    private void createNodes(GraphDatabaseAPI api, int nodeCount) {
        try (Transaction tx = api.beginTx()) {
            for(int i = 0; i < nodeCount; i++) {
                api.createNode();
            }
            tx.success();
        }
    }
}