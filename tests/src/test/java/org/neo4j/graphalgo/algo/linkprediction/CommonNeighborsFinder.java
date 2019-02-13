package org.neo4j.graphalgo.algo.linkprediction;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashSet;
import java.util.Set;

public class CommonNeighborsFinder {

    private GraphDatabaseAPI api;

    public CommonNeighborsFinder(GraphDatabaseAPI api) {
        this.api = api;
    }

    public Set<Node> findCommonNeighbors(Node node1, Node node2, RelationshipType relationshipType, Direction direction) {
        return new HashSet<>(0);
    }
}
