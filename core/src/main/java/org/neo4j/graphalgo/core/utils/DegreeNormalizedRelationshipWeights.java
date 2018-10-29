package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphdb.Direction;

/**
 * Normalized RelationshipWeights which always returns 1 / degree(source). Returns a
 * weight where - like in an probability matrix - all weights of a node add up to one.
 *
 * @author mknblch
 */
public class DegreeNormalizedRelationshipWeights implements RelationshipWeights {

    private final Degrees degrees;
    private final Direction direction;

    public DegreeNormalizedRelationshipWeights(Degrees degrees) {
        this(degrees, Direction.OUTGOING);
    }

    public DegreeNormalizedRelationshipWeights(Degrees degrees, Direction direction) {
        this.degrees = degrees;
        this.direction = direction;
    }

    @Override
    public double weightOf(int sourceNodeId, int targetNodeId) {
        return 1. / degrees.degree(sourceNodeId, direction);
    }
}
