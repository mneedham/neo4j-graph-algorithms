package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.DuplicateRelationshipsStrategy;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphdb.Direction;

import static org.neo4j.graphalgo.core.heavyweight.CypherLoadingUtils.newWeightMapping;
import static org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory.ESTIMATED_DEGREE;

public class MergedRelationships implements RelationshipConsumer {
    private final AdjacencyMatrix matrix;
    private final WeightMap relWeights;
    private boolean hasRelationshipWeights;
    private DuplicateRelationshipsStrategy duplicateRelationshipsStrategy;

    public MergedRelationships(int nodeCount, GraphSetup setup, DuplicateRelationshipsStrategy duplicateRelationshipsStrategy) {
        this.matrix = new AdjacencyMatrix(nodeCount, false, setup.tracker);
        this.relWeights = newWeightMapping(
                setup.shouldLoadRelationshipWeight(),
                setup.relationDefaultWeight,
                nodeCount * ESTIMATED_DEGREE);
        this.hasRelationshipWeights = setup.shouldLoadRelationshipWeight();
        this.duplicateRelationshipsStrategy = duplicateRelationshipsStrategy;
    }

    public boolean canMerge(Relationships result) {
        return result.rows() > 0;
    }

    public void merge(Relationships result) {
        WeightMap resultWeights = hasRelationshipWeights && result.relWeights().size() > 0 ? result.relWeights() : null;
        result.matrix().nodesWithRelationships(Direction.OUTGOING).forEachNode(
                node -> {
                    result.matrix().forEach(node, Direction.OUTGOING, getRelationshipConsumer(resultWeights));
                    return true;
                });
    }

    private RelationshipConsumer getRelationshipConsumer(WeightMap resultWeights) {
        boolean hasRelationshipWeights = resultWeights != null;
        return (source, target, relationship) -> {
            if (duplicateRelationshipsStrategy == DuplicateRelationshipsStrategy.NONE) {
                matrix.addOutgoing(source, target);
                if (hasRelationshipWeights) {
                    relWeights.put(relationship, resultWeights.get(relationship));
                }
            } else {
                boolean hasRelationship = matrix.hasOutgoing(source, target);

                if (!hasRelationship) {
                    matrix.addOutgoing(source, target);
                }

                if (hasRelationshipWeights) {
                    double oldWeight = relWeights.get(relationship, 0d);
                    double newWeight = resultWeights.get(relationship);
                    relWeights.put(relationship, hasRelationship ? duplicateRelationshipsStrategy.merge(oldWeight, newWeight) : newWeight);
                }
            }
            return true;
        };
    }

    public AdjacencyMatrix matrix() {
        return matrix;
    }

    public WeightMap relWeights() {
        return relWeights;
    }

    @Override
    public boolean accept(int sourceNodeId, int targetNodeId, long relationId) {
        return false;
    }
}
