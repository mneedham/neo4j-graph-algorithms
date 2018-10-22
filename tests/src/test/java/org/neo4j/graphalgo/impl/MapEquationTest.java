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
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.impl.infomap.MapEquationLight;
import org.neo4j.graphalgo.impl.infomap.MapEquationOpt;
import org.neo4j.graphalgo.impl.pagerank.PageRankAlgorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankResult;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.stream.LongStream;

import static org.mockito.AdditionalMatchers.eq;

/**
 * Graph:
 *
 *        (b)        (e)
 *       /  \       /  \     (x)
 *     (a)--(c)---(d)--(f)
 *
 * @author mknblch
 */
public class MapEquationTest {

    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    private static Graph graph;
    private static PageRankResult pageRankResult;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (x:Node {name:'x'})\n" + // TODO
                        "CREATE" +
                        " (a)-[:TYPE]->(b),\n" +
                        " (a)-[:TYPE]->(c),\n" +
                        " (b)-[:TYPE]->(c),\n" +

                        " (c)-[:TYPE]->(d),\n" +

                        " (d)-[:TYPE]->(e),\n" +
                        " (d)-[:TYPE]->(f),\n" +
                        " (e)-[:TYPE]->(f)";

        db.execute(cypher);

        graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .asUndirected(true)
                .load(HugeGraphFactory.class);

        pageRankResult = PageRankAlgorithm.of(graph, 1. - MapEquationOpt.TAU, LongStream.empty())
                .compute(10)
                .result();
    }

    @Test
    public void testClustering() throws Exception {

        final MapEquationOpt algo = new MapEquationOpt(graph, pageRankResult::score);

        info(algo.getCommunities(), algo.getMDL(), algo.getIndexCodeLength());

        algo.compute(10, false);


        info(algo.getCommunities(), algo.getMDL(), algo.getIndexCodeLength());

    }

    @Test
    public void testMove() throws Exception {


        final MapEquationOpt algo = new MapEquationOpt(graph, pageRankResult::score);

        info(algo.getCommunities(), algo.getMDL(), algo.getIndexCodeLength());
        algo.move(id("b"), id("a"));
        algo.move(id("c"), id("a"));

        info(algo.getCommunities(), algo.getMDL(), algo.getIndexCodeLength());
        algo.move(id("e"), id("d"));
        algo.move(id("f"), id("d"));
        info(algo.getCommunities(), algo.getMDL(), algo.getIndexCodeLength());

        algo.move(id("d"), id("a"));
        algo.move(id("e"), id("a"));
        algo.move(id("f"), id("a"));
        info(algo.getCommunities(), algo.getMDL(), algo.getIndexCodeLength());

    }

    @Test
    public void testLight() throws Exception {

        final MapEquationLight algo = new MapEquationLight(graph, pageRankResult::score);

        info(algo.getCommunities(), algo.getMDL(), algo.getIndexCodeLength());
        algo.move(id("b"), id("a"));
        algo.move(id("c"), id("a"));

        info(algo.getCommunities(), algo.getMDL(), algo.getIndexCodeLength());
        algo.move(id("e"), id("d"));
        algo.move(id("f"), id("d"));
        info(algo.getCommunities(), algo.getMDL(), algo.getIndexCodeLength());

        algo.move(id("d"), id("a"));
        algo.move(id("e"), id("a"));
        algo.move(id("f"), id("a"));
        info(algo.getCommunities(), algo.getMDL(), algo.getIndexCodeLength());

        System.out.println("algo.getModuleCodeLength() = " + algo.getModuleCodeLength());

    }

    private void info(int[] communities, double mdl, double indexCodeLength) {
        System.out.printf("%s | mdl: %.2f | clen: %.2f%n",
                Arrays.toString(communities),
                mdl,
                indexCodeLength);
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
