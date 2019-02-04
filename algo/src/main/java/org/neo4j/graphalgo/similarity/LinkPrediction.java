/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 * <p>
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 * <p>
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.*;

public class LinkPrediction {

    @Context
    public GraphDatabaseAPI api;

    @UserFunction("algo.similarity.adamicAdar")
    @Description("algo.similarity.adamicAdar([vector]) " +
            "given two nodes, calculate Adamic Adar similarity")
    public double adamicAdarSimilarity(@Name("node1") Node node1, @Name("node2") Node node2,
                                       @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        // https://en.wikipedia.org/wiki/Adamic/Adar_index

        if (node1 == null || node2 == null) {
            throw new RuntimeException("Nodes must not be null");
        }

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        RelationshipType relationshipType = configuration.getRelationship();
        Direction direction = configuration.getDirection(Direction.BOTH);

        List<Number> vector = new ArrayList<>();
        try(Transaction tx = api.beginTx()) {
            Set<Node> neighbors = findPotentialNeighbors(node1, relationshipType, direction);
            neighbors.removeIf(node -> noCommonNeighbors(node, relationshipType, direction, node2));
            neighbors.forEach(neighbor -> vector.add(degree(relationshipType, direction, neighbor)));
            tx.success();
        }

        return sumInverseLog(vector);
    }

    @UserFunction("algo.similarity.resourceAllocation")
    @Description("algo.similarity.resourceAllocation([vector]) " +
            "given two nodes, calculate Resource Allocation similarity")
    public double resourceAllocationSimilarity(@Name("node1") Node node1, @Name("node2") Node node2,
                                       @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        RelationshipType relationshipType = configuration.getRelationship();
        Direction direction = configuration.getDirection(Direction.BOTH);

        List<Number> vector = new ArrayList<>();
        try(Transaction tx = api.beginTx()) {
            Set<Node> neighbors = findPotentialNeighbors(node1, relationshipType, direction);
            neighbors.removeIf(node -> noCommonNeighbors(node, relationshipType, direction, node2));
            neighbors.forEach(neighbor -> vector.add(degree(relationshipType, direction, neighbor)));
            tx.success();
        }

        return sumInverse(vector);
    }


    public double sumInverse(@Name("vector") List<Number> vector) {
        // https://arxiv.org/pdf/0901.0553.pdf
        if (vector == null) return 0;

        double score = 0.0;
        for (int i = 0; i < vector.size(); i++) {
            double value = vector.get(i).doubleValue();
            score += 1 / value;
        }
        return score;
    }

    public double sumInverseLog(@Name("vector") List<Number> vector) {
        // https://en.wikipedia.org/wiki/Adamic/Adar_index
        if (vector == null) return 0;

        double score = 0.0;
        for (int i = 0; i < vector.size(); i++) {
            double value = vector.get(i).doubleValue();
            score += 1 / Math.log(value);
        }
        return score;
    }


    private Set<Node> findPotentialNeighbors(@Name("node1") Node node1, RelationshipType relationshipType, Direction direction) {
        Set<Node> neighbors = new HashSet<>();

        for (Relationship rel : loadRelationships(node1, relationshipType, direction)) {
            Node endNode = rel.getEndNode();
            neighbors.add(endNode);
        }
        return neighbors;
    }

    private int degree(RelationshipType relationshipType, Direction direction, Node node) {
        return relationshipType == null ? node.getDegree(direction) : node.getDegree(relationshipType, direction);
    }

    private Iterable<Relationship> loadRelationships(Node node, RelationshipType relationshipType, Direction direction) {
        return relationshipType == null ? node.getRelationships(direction) : node.getRelationships(relationshipType, direction);
    }

    private boolean noCommonNeighbors(Node node, RelationshipType relationshipType, Direction direction, Node node2) {
        for(Relationship rel : loadRelationships(node, relationshipType, direction)) {
            if(rel.getOtherNode(node).equals(node2)) {
                return false;
            }
        }
        return true;
    }


}
