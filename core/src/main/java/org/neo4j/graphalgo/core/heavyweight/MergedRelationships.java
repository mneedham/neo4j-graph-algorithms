package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphdb.Direction;

import static org.neo4j.graphalgo.core.heavyweight.CypherLoadingUtils.newWeightMapping;
import static org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory.ESTIMATED_DEGREE;

public class MergedRelationships {
    private final AdjacencyMatrix matrix;
    private final WeightMap relWeights;
    private boolean hasRelationshipWeights;
    private boolean accumulateWeights;

    public MergedRelationships(int nodeCount, GraphSetup setup, boolean accumulateWeights) {
        this.matrix = new AdjacencyMatrix(nodeCount, false, setup.tracker);
        this.relWeights = newWeightMapping(
                setup.shouldLoadRelationshipWeight(),
                setup.relationDefaultWeight,
                nodeCount * ESTIMATED_DEGREE);
        this.hasRelationshipWeights = setup.shouldLoadRelationshipWeight();
        this.accumulateWeights = accumulateWeights;
    }

    public boolean canMerge(Relationships result) {
        return result.rows() > 0;
    }

    public void merge(Relationships result) {
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
                                    if(!matrix.hasOutgoing(source, target)) {
                                        matrix.addOutgoing(source, target);
                                        if (resultWeights != null) {
                                            relWeights.put(relationship, resultWeights.get(relationship));
                                        }
                                    }
                                }
                                return true;
                            });
                    return true;
                });
    }

    public AdjacencyMatrix matrix() {
        return matrix;
    }

    public WeightMap relWeights() {
        return relWeights;
    }
}
