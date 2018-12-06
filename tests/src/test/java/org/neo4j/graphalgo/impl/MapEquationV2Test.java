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
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.DegreeNormalizedRelationshipWeights;
import org.neo4j.graphalgo.core.utils.GraphNormalizedRelationshipWeights;
import org.neo4j.graphalgo.impl.infomap.*;
import org.neo4j.graphalgo.impl.pagerank.PageRankAlgorithm;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.stream.LongStream;

/**
 * Graph:
 *
 *        (b)        (e)
 *       /  \       /  \     (x)
 *     (a)--(c)---(d)--(f)
 *
 * @author mknblch
 */
public class MapEquationV2Test {

    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    private static Graph graph;
    private static NodeWeights pageRankResult;
    private static RelationshipWeights normalizedWeights;

    @BeforeClass
    public static void setupGraph() throws KernelException {

//        final String cypher =
//                "CREATE (a:Node {name:'a'})\n" +
//                        "CREATE (b:Node {name:'b'})\n" +
//                        "CREATE (c:Node {name:'c'})\n" +
//                        "CREATE (d:Node {name:'d'})\n" +
//                        "CREATE (e:Node {name:'e'})\n" +
//                        "CREATE (f:Node {name:'f'})\n" +
//                        "CREATE (x:Node {name:'x'})\n" +
//                        "CREATE" +
//                        " (b)-[:TYPE]->(a),\n" +
//                        " (a)-[:TYPE]->(c),\n" +
//                        " (c)-[:TYPE]->(a),\n" +
//
//                        " (d)-[:TYPE]->(c),\n" +
//
//                        " (d)-[:TYPE]->(e),\n" +
//                        " (d)-[:TYPE]->(f),\n" +
//                        " (e)-[:TYPE]->(f)";

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (c:Node {name:'c'})\n" + // shuffled
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (h:Node {name:'h'})\n" +
                        "CREATE (z:Node {name:'z'})\n" +

                        "CREATE" +

                        " (a)-[:TYPE]->(b),\n" +
                        " (a)-[:TYPE]->(c),\n" +
                        " (a)-[:TYPE]->(d),\n" +
                        " (b)-[:TYPE]->(c),\n" +
                        " (c)-[:TYPE]->(d),\n" +
                        " (b)-[:TYPE]->(d),\n" +

                        " (f)-[:TYPE]->(e),\n" +
                        " (e)-[:TYPE]->(h),\n" +
                        " (e)-[:TYPE]->(g),\n" +
                        " (f)-[:TYPE]->(g),\n" +
                        " (f)-[:TYPE]->(h),\n" +
                        " (g)-[:TYPE]->(h),\n" +
                        " (b)-[:TYPE]->(e)";
        db.execute(cypher);

        graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .asUndirected(true)
//                .withDirection(Direction.BOTH)
                .load(HeavyGraphFactory.class);

        // equal PRs leads to wrong results
//        final long nodeCount = graph.nodeCount();
//        pageRankResult = n -> .1 / nodeCount;

        pageRankResult = new SimplePageRank(graph, 1. - MapEquation.TAU)
                .compute(10);

        // confusing results due to PR(j) is not normalized
//        pageRankResult = PageRankAlgorithm.of(graph, 1. - MapEquation.TAU, LongStream.empty())
//                .compute(10)
//                .result()::score;

        normalizedWeights = new GraphNormalizedRelationshipWeights(graph, graph, (s, t) -> 1.);

//        normalizedWeights = new DegreeNormalizedRelationshipWeights(graph, Direction.OUTGOING);

//        normalizedWeights = new DegreeNormalizedRelationshipWeights(graph, Direction.BOTH);


    }


    @Test
    public void testMove() throws Exception {

        final MapEquationV2 algo = new MapEquationV2(graph, pageRankResult, normalizedWeights);

        info(algo);

        algo.merge(id("a"), id("b"));
        algo.merge(id("a"), id("c"));
        algo.merge(id("a"), id("d"));

        info(algo);
        algo.merge(id("h"), id("e"));
        algo.merge(id("h"), id("f"));
        algo.merge(id("h"), id("g"));
        info(algo);

        algo.merge(id("a"), id("h"));
        info(algo);

    }

    @Test
    public void test2() throws Exception {

        final MapEquationV2 algo = new MapEquationV2(graph, pageRankResult, normalizedWeights);

        algo.compute();

        info(algo);
    }

    @Test
    public void testPlogp() throws Exception {
        for (double i = -2.; i < 5.; i += 0.1) {
            System.out.printf("plogp(%2.2f) = %.2f%n", i, MapEquationAlgorithm.plogp(i));
        }
    }

    @Test
    public void testLg2() throws Exception {
        for (double i = -2.; i < 5.; i += 0.1) {
            System.out.printf("log2(%2.2f) = %.2f%n", i, MapEquationAlgorithm.log2(i));
        }
    }




    private void info(MapEquationAlgorithm algo) {
        System.out.printf("%s | mdl: %5.4f | icl: %5.4f | mcl: %5.4f | i: %d%n",
                Arrays.toString(algo.getCommunities()),
                algo.getMDL(),
                algo.getIndexCodeLength(),
                algo.getModuleCodeLength(),
                algo.getIterations());
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
