package org.neo4j.graphalgo.core.heavyweight;

import org.junit.Test;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import static org.junit.Assert.assertEquals;

public class MergedRelationshipsTest {
    @Test
    public void accumulatingRemovesDuplicates() {
        GraphSetup setup = new GraphSetup();

        AdjacencyMatrix matrix = new AdjacencyMatrix(2, false, AllocationTracker.EMPTY);
        matrix.addOutgoing(0, 1);

        WeightMap relWeights = new WeightMap(10, 0, 0);
        Relationships relationships = new Relationships(0, 5, matrix, relWeights, 0.0);

        MergedRelationships mergedRelationships = new MergedRelationships(5, setup, true);

        mergedRelationships.merge(relationships);
        mergedRelationships.merge(relationships);

        assertEquals(1, mergedRelationships.matrix().degree(0, Direction.OUTGOING));
    }

    @Test
    public void nonAccumulatingRemovesDuplicates() {
        GraphSetup setup = new GraphSetup();

        AdjacencyMatrix matrix = new AdjacencyMatrix(2, false, AllocationTracker.EMPTY);
        matrix.addOutgoing(0, 1);

        WeightMap relWeights = new WeightMap(10, 0, 0);
        Relationships relationships = new Relationships(0, 5, matrix, relWeights, 0.0);

        MergedRelationships mergedRelationships = new MergedRelationships(5, setup, false);

        mergedRelationships.merge(relationships);
        mergedRelationships.merge(relationships);

        assertEquals(1, mergedRelationships.matrix().degree(0, Direction.OUTGOING));
    }


}
