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
package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.DegreeNormalizedRelationshipWeights;
import org.neo4j.graphalgo.impl.infomap.MapEquation;
import org.neo4j.graphalgo.impl.infomap.MapEquationAlgorithm;
import org.neo4j.graphalgo.impl.infomap.MapEquationOpt1;
import org.neo4j.graphalgo.impl.infomap.SimplePageRank;
import org.neo4j.graphalgo.impl.pagerank.PageRankAlgorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankResult;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;

/**
 * Graph:
 *
 *        (b)        (e)
 *       /  \       /  \     (x)
 *     (a)--(c)---(d)--(f)
 *
 * @author mknblch
 */
public class SimplePageRankTest {

    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    private static Graph graph;

    @BeforeClass
    public static void setupGraph() throws KernelException {


        final String cypher = "" +
                "CREATE (a:Node {name:\"a\"})\n" +
                "CREATE (b:Node {name:\"b\"})\n" +
                "CREATE (c:Node {name:\"c\"})\n" +
                "CREATE (d:Node {name:\"d\"})\n" +
                "CREATE (e:Node {name:\"e\"})\n" +
                "CREATE (f:Node {name:\"f\"})\n" +
                "CREATE (g:Node {name:\"g\"})\n" +
                "CREATE (h:Node {name:\"h\"})\n" +
                "CREATE (i:Node {name:\"i\"})\n" +
                "CREATE (j:Node {name:\"j\"})\n" +
                "CREATE (k:Node {name:\"k\"})\n" +
                "CREATE\n" +
                // a (dangling node)
                // b
                "  (b)-[:TYPE]->(c),\n" +
                // c
                "  (c)-[:TYPE]->(b),\n" +
                // d
                "  (d)-[:TYPE]->(a),\n" +
                "  (d)-[:TYPE]->(b),\n" +
                // e
                "  (e)-[:TYPE]->(b),\n" +
                "  (e)-[:TYPE]->(d),\n" +
                "  (e)-[:TYPE]->(f),\n" +
                // f
                "  (f)-[:TYPE]->(b),\n" +
                "  (f)-[:TYPE]->(e),\n" +
                // g
                "  (g)-[:TYPE]->(b),\n" +
                "  (g)-[:TYPE]->(e),\n" +
                // h
                "  (h)-[:TYPE]->(b),\n" +
                "  (h)-[:TYPE]->(e),\n" +
                // i
                "  (i)-[:TYPE]->(b),\n" +
                "  (i)-[:TYPE]->(e),\n" +
                // j
                "  (j)-[:TYPE]->(e),\n" +
                // k
                "  (k)-[:TYPE]->(e)\n";

        db.execute(cypher);

        graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .asUndirected(true)
                .load(HeavyGraphFactory.class);
    }

    @Test
    public void test() throws Exception {
        final double[] pageRanks = new SimplePageRank(graph, 0.85)
                .compute(10)
                .getPageRanks();

        assertEquals(0.314, pageRanks[1], 0.001);
    }


    private int id(String name) {
        final Node[] node = new Node[1];
        db.execute("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n").accept(row -> {
            node[0] = row.getNode("n");
            return false;
        });
        return graph.toMappedNodeId(node[0].getId());
    }
}
