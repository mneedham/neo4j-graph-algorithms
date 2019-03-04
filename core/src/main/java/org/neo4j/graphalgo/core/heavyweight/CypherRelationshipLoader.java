package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.neo4j.graphalgo.core.heavyweight.CypherLoadingUtils.newWeightMapping;
import static org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory.NO_BATCH;

public class CypherRelationshipLoader {
    private final GraphDatabaseAPI api;
    private final GraphSetup setup;

    public CypherRelationshipLoader(GraphDatabaseAPI api, GraphSetup setup) {
        this.api = api;
        this.setup = setup;
    }

    public Relationships load(Nodes nodes) {
        int batchSize = setup.batchSize;
        return CypherLoadingUtils.canBatchLoad(setup.loadConcurrent(), batchSize, setup.relationshipType) ?
                batchLoadRelationships(batchSize, nodes) :
                loadRelationships(0, NO_BATCH, nodes);
    }

    private Relationships batchLoadRelationships(int batchSize, Nodes nodes) {
        ExecutorService pool = setup.executor;
        int threads = setup.concurrency();
        int nodeCount = nodes.idMap.size();

        MergedRelationships mergedRelationships = new MergedRelationships(nodeCount, setup, setup.duplicateRelationshipsStrategy);

        long offset = 0;
        long lastOffset = 0;
        long total = 0;
        List<Future<Relationships>> futures = new ArrayList<>(threads);
        boolean working = true;
        do {
            long skip = offset;
            // suboptimal, each sub-call allocates a AdjacencyMatrix of nodeCount size, would be better with a sparse variant
            futures.add(pool.submit(() -> loadRelationships(skip, batchSize, nodes)));
            offset += batchSize;
            if (futures.size() >= threads) {
                for (Future<Relationships> future : futures) {
                    Relationships result = CypherLoadingUtils.get(
                            "Error during loading relationships offset: " + (lastOffset + batchSize),
                            future);
                    lastOffset = result.offset();
                    total += result.rows();
                    working = mergedRelationships.canMerge(result);
                    if (working) {
                        mergedRelationships.merge(result);
                    }
                }
                futures.clear();
            }
        } while (working);

        return new Relationships(0, total, mergedRelationships.matrix(), mergedRelationships.relWeights(), setup.relationDefaultWeight);
    }

    private Relationships loadRelationships(long offset, int batchSize, Nodes nodes) {

        IdMap idMap = nodes.idMap;

        int nodeCount = idMap.size();
        int capacity = batchSize == NO_BATCH ? nodeCount : batchSize;

        final AdjacencyMatrix matrix = new AdjacencyMatrix(nodeCount, false, setup.tracker);

        boolean hasRelationshipWeights = setup.shouldLoadRelationshipWeight();
        final WeightMap relWeights = newWeightMapping(hasRelationshipWeights, setup.relationDefaultWeight, capacity);

        RelationshipRowVisitor visitor = new RelationshipRowVisitor(idMap, hasRelationshipWeights, relWeights, matrix, setup.duplicateRelationshipsStrategy);
        api.execute(setup.relationshipType, CypherLoadingUtils.params(setup.params, offset, batchSize)).accept(visitor);
        return new Relationships(offset, visitor.rows(), matrix, relWeights, setup.relationDefaultWeight);
    }


}
