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
package org.neo4j.graphalgo.algo.similarity;

import org.junit.Test;
import org.neo4j.graphalgo.similarity.Similarities;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class ResourceAllocationSimilarityTest {
    @Test
    public void uniqueCommonNeighbour() {
        assertEquals(0.5, new Similarities().sumInverse(Collections.singletonList(2)), 0.01);
        assertEquals(1.0, new Similarities().sumInverse(Arrays.asList(2,2 )), 0.01);
    }

    @Test
    public void veryWellLinkedNeighbours() {
        assertEquals(0.2, new Similarities().sumInverse(Collections.singletonList(5)), 0.01);
        assertEquals(0.4, new Similarities().sumInverse(Arrays.asList(5,5)), 0.01);
    }

}
