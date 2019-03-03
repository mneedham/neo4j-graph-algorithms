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

import org.neo4j.graphalgo.core.utils.Intersections;

import java.util.Arrays;



public class CategoricalInput implements  Comparable<CategoricalInput>, SimilarityInput {
    long id;
    long[] targets;

    public CategoricalInput(long id, long[] targets) {
        this.id = id;
        this.targets = targets;
    }

    public long getId() {
        return id;
    }

    @Override
    public int compareTo(CategoricalInput o) {
        return Long.compare(id, o.id);
    }

    SimilarityResult jaccard(double similarityCutoff, CategoricalInput e2, boolean bidirectional) {
        long intersection = Intersections.intersection3(targets, e2.targets);
        if (similarityCutoff >= 0d && intersection == 0) return null;
        int count1 = targets.length;
        int count2 = e2.targets.length;
        long denominator = count1 + count2 - intersection;
        double jaccard = denominator == 0 ? 0 : (double) intersection / denominator;
        if (jaccard < similarityCutoff) return null;
        return new SimilarityResult(id, e2.id, count1, count2, intersection, jaccard, bidirectional, false);
    }

    SimilarityResult overlap(double similarityCutoff, CategoricalInput e2) {
    return overlap(similarityCutoff, e2, true);
    }

    SimilarityResult overlap(double similarityCutoff, CategoricalInput e2, boolean inferReverse) {
        long intersection = Intersections.intersection3(targets, e2.targets);
        if (similarityCutoff >= 0d && intersection == 0) return null;
        int count1 = targets.length;
        int count2 = e2.targets.length;
        long denominator = Math.min(count1, count2);
        double overlap = denominator == 0 ? 0 : (double) intersection / denominator;
        if (overlap < similarityCutoff) return null;

        if (count1 <= count2) {
            return new SimilarityResult(id, e2.id, count1, count2, intersection, overlap, false, false);
        } else {
            return inferReverse ? new SimilarityResult(e2.id, id, count2, count1, intersection, overlap, false, true) : null;
        }

    }

    @Override
    public String toString() {
        return "CategoricalInput{" +
                "id=" + id +
                ", targets=" + Arrays.toString(targets) +
                '}';

    }
}
