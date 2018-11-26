package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.IntDoubleMap;
import com.carrotsearch.hppc.IntDoubleScatterMap;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphdb.Direction;

/**
 * @author mknblch
 */
public class GraphNormalizedRelationshipWeights implements RelationshipWeights {

    private final double weightSum;
    private RelationshipWeights weights;

    public GraphNormalizedRelationshipWeights(NodeIterator nodeIterator, RelationshipIterator relationshipIterator, RelationshipWeights weights) {
        this.weights = weights;
        final Pointer.DoublePointer sum = Pointer.wrap(.0);
        nodeIterator.forEachNode(n -> {
            relationshipIterator.forEachRelationship(n, Direction.OUTGOING, (s, t, r) -> {
                sum.v += weights.weightOf(s, t);
                return true;
            });
            return true;
        });
        weightSum = sum.v;
    }

    @Override
    public double weightOf(int sourceNodeId, int targetNodeId) {
        return weights.weightOf(sourceNodeId, targetNodeId) / weightSum;
    }

    public void release() {
        weights = null;
    }
}
