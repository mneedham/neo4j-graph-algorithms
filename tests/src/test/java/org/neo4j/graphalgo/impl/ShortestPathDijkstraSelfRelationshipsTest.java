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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphdb.*;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public final class ShortestPathDijkstraSelfRelationshipsTest {

    // https://en.wikipedia.org/wiki/Shortest_path_problem#/media/File:Shortest_path_with_direct_weights.svg
    private static final String DB_CYPHER = "" +
            "CREATE (a:Label1 {name:\"a\"})\n" +
            "CREATE (b:Label1 {name:\"b\"})\n" +
            "CREATE (c:Label1 {name:\"c\"})\n" +
            "CREATE (d:Label1 {name:\"d\"})\n" +
            "CREATE (e:Label1 {name:\"e\"})\n" +
            "CREATE (f:Label1 {name:\"f\"})\n" +
            "CREATE (g:Label1 {name:\"g\"})\n" +
            "CREATE\n" +

            "  (g)-[:TYPE4 {cost:4}]->(c),\n" +
            "  (b)-[:TYPE5 {cost:4}]->(g),\n" +
            "  (g)-[:ZZZZ {cost:4}]->(g),\n" +
            "  (g)-[:TYPE6 {cost:4}]->(d),\n" +
            "  (b)-[:TYPE6 {cost:4}]->(g),\n" +
            "  (g)-[:TYPE99 {cost:4}]->(g)";

    /*
    +----------------------------------+
    | id(a) | id(b) | relId | type(r)  |
    +----------------------------------+
    | 6     | 2     | 0     | "TYPE4"  |
    | 1     | 6     | 1     | "TYPE5"  |
    | 6     | 6     | 2     | "ZZZZ"   |
    | 6     | 3     | 3     | "TYPE6"  |
    | 1     | 6     | 4     | "TYPE6"  |
    | 6     | 6     | 5     | "TYPE99" |
    +----------------------------------+
     */

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
       return  Collections.singletonList(new Object[]{HeavyGraphFactory.class, "HeavyGraphFactory"});
    }

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() {
        System.out.println(DB_CYPHER);
        DB.execute(DB_CYPHER).close();
    }

    private Class<? extends GraphFactory> graphImpl;



    public ShortestPathDijkstraSelfRelationshipsTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        this.graphImpl = graphImpl;
    }

    @Test
    public void testResultStream() {
        final Label label = Label.label("Label1");
        RelationshipType type = RelationshipType.withName("TYPE1");
        ShortestPath expected = expected(label, type,
                "name", "a",
                "name", "c",
                "name", "e",
                "name", "d",
                "name", "f");
        final long head = expected.nodeIds[0], tail = expected.nodeIds[expected.nodeIds.length - 1];

        final Graph graph = new GraphLoader(DB)
                .withLabel("Foo|Bar")
                .withRelationshipType("Bar|Foo")
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .withDirection(Direction.OUTGOING)
                .asUndirected(true)
                .load(graphImpl);

        final ShortestPathDijkstra shortestPathDijkstra = new ShortestPathDijkstra(graph);
        Stream<ShortestPathDijkstra.Result> resultStream = shortestPathDijkstra
                .compute(head, tail, Direction.OUTGOING)
                .resultStream();

        assertEquals(expected.weight, shortestPathDijkstra.getTotalCost(), 0.1);
        assertEquals(expected.nodeIds.length, resultStream.count());
    }

    private static ShortestPath expected(
            Label label,
            RelationshipType type,
            String... kvPairs) {
        return DB.executeAndCommit(db -> {
            double weight = 0.0;
            Node prev = null;
            long[] nodeIds = new long[kvPairs.length / 2];
            for (int i = 0; i < nodeIds.length; i++) {
                Node current = db.findNode(label, kvPairs[2*i], kvPairs[2*i + 1]);
                long id = current.getId();
                nodeIds[i] = id;
                if (prev != null) {
                    for (Relationship rel : prev.getRelationships(type, Direction.OUTGOING)) {
                        if (rel.getEndNodeId() == id) {
                            double cost = ((Number) rel.getProperty("cost")).doubleValue();
                            weight += cost;
                        }
                    }
                }
                prev = current;
            }

            return new ShortestPath(nodeIds, weight);
        });
    }

    private static final class ShortestPath {
        private final long[] nodeIds;
        private final double weight;

        private ShortestPath(long[] nodeIds, double weight) {
            this.nodeIds = nodeIds;
            this.weight = weight;
        }
    }
}
