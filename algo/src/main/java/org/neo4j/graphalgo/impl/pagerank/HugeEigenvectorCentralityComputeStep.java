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

import org.neo4j.graphalgo.api.HugeDegrees;
import org.neo4j.graphalgo.api.HugeRelationshipConsumer;
import org.neo4j.graphalgo.api.HugeRelationshipIterator;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

final class HugeEigenvectorCentralityComputeStep extends HugeBaseComputeStep implements HugeRelationshipConsumer {
    private final HugeRelationshipIterator relationshipIterator;
    private final long nodeCount;

    HugeEigenvectorCentralityComputeStep(
            double dampingFactor,
            long[] sourceNodeIds,
            HugeRelationshipIterator relationshipIterator,
            HugeDegrees degrees,
            AllocationTracker tracker,
            int partitionSize,
            long startNode,
            long nodeCount) {
        super(dampingFactor,
                sourceNodeIds,
                relationshipIterator,
                degrees,
                tracker,
                partitionSize,
                startNode);
        this.relationshipIterator = relationshipIterator;
        this.nodeCount = nodeCount;
    }


    private int srcRankDelta;

    @Override
    protected double initialValue() {
        return 1.0 / nodeCount;
    }

    void singleIteration() {
        long startNode = this.startNode;
        long endNode = this.endNode;
        HugeRelationshipIterator rels = this.relationshipIterator;
        for (long nodeId = startNode; nodeId < endNode; ++nodeId) {
            double delta = deltas[(int) (nodeId - startNode)];
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
    void combineScores() {
        assert prevScores != null;
        assert prevScores.length >= 1;

        int scoreDim = prevScores.length;
        int[][] prevScores = this.prevScores;

        int length = prevScores[0].length;
        for (int i = 0; i < length; i++) {
            int sum = 0;
            for (int j = 0; j < scoreDim; j++) {
                int[] scores = prevScores[j];
                sum += scores[i];
                scores[i] = 0;
            }
            double delta = (sum / 100_000.0);
            pageRank[i] += delta;
            deltas[i] = delta;
        }

        normalizeDeltas();
    }

    private void normalizeDeltas() {
        double norm = computeNorm(deltas);
        for (int i = 0; i < deltas.length; i++) {
            deltas[i] = deltas[i] / norm;
        }
    }

    private double computeNorm(double[] allScores) {
        double norm = Arrays.stream(allScores).map(score -> score * score).sum();
        return norm < 0.0 ? 1.0 : norm;
    }

    public boolean accept(long sourceNodeId, long targetNodeId) {
        if (srcRankDelta != 0) {
            int idx = binaryLookup(targetNodeId, starts);
            nextScores[idx][(int) (targetNodeId - starts[idx])] += srcRankDelta;
        }
        return true;
    }
}
