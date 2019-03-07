/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

final class EigenvectorCentralityComputeStep extends BaseComputeStep implements RelationshipConsumer   {

    private final RelationshipIterator relationshipIterator;

    EigenvectorCentralityComputeStep(
            int[] sourceNodeIds,
            RelationshipIterator relationshipIterator,
            Degrees degrees,
            int partitionSize,
            int startNode) {
        super(1.0, sourceNodeIds, degrees, partitionSize, startNode);
        this.relationshipIterator = relationshipIterator;
    }


    private int srcRankDelta;


    void singleIteration() {
        int startNode = this.startNode;
        int endNode = this.endNode;
        RelationshipIterator rels = this.relationshipIterator;
        for (int nodeId = startNode; nodeId < endNode; ++nodeId) {
            double delta = deltas[nodeId - startNode];
            if (delta > 0) {
                int degree = degrees.degree(nodeId, Direction.OUTGOING);
                if (degree > 0) {
                    srcRankDelta = (int) (100_000 * delta);
                    rels.forEachRelationship(nodeId, Direction.OUTGOING, this);
                }
            }
        }
    }

    @Override
    protected double initialValue() {
        return 1.0 / partitionSize;
    }

    void synchronizeScores(int[] allScores) {
        double[] pageRank = this.pageRank;

        int length = allScores.length;
        for (int i = 0; i < length; i++) {
            int sum = allScores[i];

            double delta = (sum / 100_000.0);
            pageRank[i] += delta;
            deltas[i] = delta;
            allScores[i] = 0;
        }

        double norm = computeNorm(deltas);
        for (int i = 0; i < deltas.length; i++) {
            deltas[i] = deltas[i] / norm;
        }

//        double pageRankNorm = computeNorm(pageRank);
//        for (int i = 0; i < pageRank.length; i++) {
//            pageRank[i] = pageRank[i] / pageRankNorm;
//        }

    }

    private double computeNorm(double[] allScores) {
        double norm = Arrays.stream(allScores).map(score -> score * score).sum();
        return norm < 0.0 ? 1.0 : norm;
    }

    @Override
    public boolean accept(int sourceNodeId, int targetNodeId, long relationId) {
        if (srcRankDelta != 0) {
            int idx = binaryLookup(targetNodeId, starts);
            nextScores[idx][targetNodeId - starts[idx]] += srcRankDelta;
        }
        return true;
    }
}
