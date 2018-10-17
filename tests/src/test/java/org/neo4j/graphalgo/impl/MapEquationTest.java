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

import com.carrotsearch.hppc.LongArrayList;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.impl.infomap.MapEquation;
import org.neo4j.graphalgo.impl.infomap.MapEquationAlgorithm;
import org.neo4j.graphalgo.impl.infomap.MapEquationLight;
import org.neo4j.graphalgo.impl.pagerank.PageRank;
import org.neo4j.graphalgo.impl.pagerank.PageRankAlgorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankResult;
import org.neo4j.graphalgo.impl.pagerank.PageRankVariant;
import org.neo4j.graphalgo.impl.yens.Dijkstra;
import org.neo4j.graphalgo.impl.yens.WeightedPath;
import org.neo4j.graphalgo.impl.yens.YensKShortestPaths;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.List;
import java.util.Optional;
import java.util.function.DoubleConsumer;
import java.util.stream.LongStream;

import static org.junit.Assert.*;
import static org.mockito.AdditionalMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Graph:
 *
 *        (b)        (e)
 *       /  \       /  \
 *     (a)--(c)---(d)--(f)
 *
 * @author mknblch
 */
public class MapEquationTest {

    public static final double DELTA = 0.001;

    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    private static Graph graph;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
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
    }

    @Test
    public void test() throws Exception {

        final MapEquation algo = new MapEquationAlgorithm(graph).build();

        info(algo);

        algo.move(id("b"), id("a"));
        algo.move(id("c"), id("a"));

        algo.move(id("e"), id("d"));
        algo.move(id("f"), id("d"));

        info(algo);

        algo.move(id("e"), id("a"));
        algo.move(id("f"), id("a"));
        algo.move(id("d"), id("a"));

        info(algo);

    }


    @Test
    public void testLight() throws Exception {

        final MapEquationLight algo = new MapEquationAlgorithm(graph).light();

        info(algo);

        algo.move(id("b"), id("a"));
        algo.move(id("c"), id("a"));

        algo.move(id("e"), id("d"));
        algo.move(id("f"), id("d"));

        info(algo);

        algo.move(id("e"), id("a"));
        algo.move(id("f"), id("a"));
        algo.move(id("d"), id("a"));

        info(algo);

    }

    private void info(MapEquation algo) {
        System.out.println("algo.getMDL() = " + algo.getMDL());
        System.out.println("algo.getIndexCodeLength() = " + algo.getIndexCodeLength());
        System.out.println("algo.getModuleCodeLength() = " + algo.getModuleCodeLength());
        algo.forEachModule(m -> {
//            System.out.println("\tm.qOut() = " + m.qOut());
            System.out.println("\tm.getCodeBookLength() = " + m.getCodeBookLength());
        });
    }

    private void info(MapEquationLight algo) {
        System.out.println("algo.getMDL() = " + algo.getMDL());
        System.out.println("algo.getIndexCodeLength() = " + algo.getIndexCodeLength());
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
