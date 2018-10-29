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
public class NormalizedRelationshipWeights implements RelationshipWeights {

    private RelationshipWeights weights;
    private IntDoubleMap nodeWeightSum;

    public NormalizedRelationshipWeights(NodeIterator nodeIterator, RelationshipIterator relationshipIterator, RelationshipWeights weights) {
        this.weights = weights;
        nodeWeightSum = new IntDoubleScatterMap();
        nodeIterator.forEachNode(n -> {
            relationshipIterator.forEachRelationship(n, Direction.OUTGOING, (s, t, r) -> {
                nodeWeightSum.addTo(n, weights.weightOf(s, t));
                return true;
            });
            return true;
        });
    }

    @Override
    public double weightOf(int sourceNodeId, int targetNodeId) {
        return weights.weightOf(sourceNodeId, targetNodeId) / nodeWeightSum.getOrDefault(sourceNodeId, 1.);
    }

    public void release() {
        nodeWeightSum.clear();
        nodeWeightSum = null;
        weights = null;
    }
}
