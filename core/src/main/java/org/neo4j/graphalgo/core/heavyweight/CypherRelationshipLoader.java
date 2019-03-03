package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.neo4j.graphalgo.core.heavyweight.CypherLoadingUtils.newWeightMapping;
import static org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory.*;

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
        boolean accumulateWeights = setup.accumulateWeights;

        // data structures for merged information
        int nodeCount = nodes.idMap.size();
        AdjacencyMatrix matrix = new AdjacencyMatrix(nodeCount, false, setup.tracker);
        boolean hasRelationshipWeights = setup.shouldLoadRelationshipWeight();
        final WeightMap relWeights = newWeightMapping(
                hasRelationshipWeights,
                setup.relationDefaultWeight,
                nodeCount * ESTIMATED_DEGREE);

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
                    working = result.rows() > 0;
                    if (working) {
                        WeightMap resultWeights = hasRelationshipWeights && result.relWeights().size() > 0 ? result.relWeights() : null;
                        result.matrix().nodesWithRelationships(Direction.OUTGOING).forEachNode(
                                node -> {
                                    result.matrix().forEach(node, Direction.OUTGOING,
                                            (source, target, relationship) -> {
                                                if (accumulateWeights) {
                                                    // suboptimial, O(n) per node
                                                    if (!matrix.hasOutgoing(source, target)) {
                                                        matrix.addOutgoing(source, target);
                                                    }
                                                    if (resultWeights != null) {
                                                        double oldWeight = relWeights.get(relationship, 0d);
                                                        double newWeight = resultWeights.get(relationship) + oldWeight;
                                                        relWeights.put(relationship, newWeight);
                                                    }
                                                } else {
                                                    matrix.addOutgoing(source, target);
                                                    if (resultWeights != null) {
                                                        relWeights.put(relationship, resultWeights.get(relationship));
                                                    }
                                                }
                                                return true;
                                            });
                                    return true;
                                });
                    }
                }
                futures.clear();
            }
        } while (working);

        return new Relationships(0, total, matrix, relWeights, setup.relationDefaultWeight);
    }

    private Relationships loadRelationships(long offset, int batchSize, Nodes nodes) {

        IdMap idMap = nodes.idMap;

        int nodeCount = idMap.size();
        int capacity = batchSize == NO_BATCH ? nodeCount : batchSize;

        final AdjacencyMatrix matrix = new AdjacencyMatrix(nodeCount, false, setup.tracker);

        boolean hasRelationshipWeights = setup.shouldLoadRelationshipWeight();
        final WeightMap relWeights = newWeightMapping(hasRelationshipWeights, setup.relationDefaultWeight, capacity);

        RelationshipRowVisitor visitor = new RelationshipRowVisitor(idMap, hasRelationshipWeights, relWeights, matrix, setup.accumulateWeights);
        api.execute(setup.relationshipType, CypherLoadingUtils.params(setup.params, offset, batchSize)).accept(visitor);
        return new Relationships(offset, visitor.rows(), matrix, relWeights, setup.relationDefaultWeight);
    }


}
