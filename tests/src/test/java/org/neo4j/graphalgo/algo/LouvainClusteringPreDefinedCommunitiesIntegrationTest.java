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
package org.neo4j.graphalgo.algo;

import com.carrotsearch.hppc.IntIntScatterMap;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.neo4j.graphalgo.LouvainProc;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Graph:
 *
 * (a)-(b)---(e)-(f)
 *  | X |     | X |   (z)
 * (c)-(d)   (g)-(h)
 *
 * @author mknblch
 */
public class LouvainClusteringPreDefinedCommunitiesIntegrationTest {

    private final static String[] NODES = {"a", "b", "c", "d", "e", "f", "g", "h", "z"};

    @ClassRule
    public static ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "MERGE (nAlice:User {id:'Alice'}) SET nAlice.community = 0\n" +
                        "MERGE (nBridget:User {id:'Bridget'}) SET nBridget.community = 0\n" +
                        "MERGE (nCharles:User {id:'Charles'}) SET nCharles.community = 1\n" +
                        "MERGE (nDoug:User {id:'Doug'}) SET nDoug.community = 1\n" +
                        "MERGE (nMark:User {id:'Mark'}) SET nMark.community = 1\n" +
                        "MERGE (nMichael:User {id:'Michael'}) SET nMichael.community = 0\n" +
                        "MERGE (nKarin:User {id:'Karin'}) SET nKarin.community = 1\n" +
                        "MERGE (nAmy:User {id:'Amy'}) SET nAmy.community = 100\n" +
                        "\n" +
                        "MERGE (nAlice)-[:FRIEND]->(nBridget)\n" +
                        "MERGE (nAlice)-[:FRIEND]->(nCharles)\n" +
                        "MERGE (nMark)-[:FRIEND]->(nDoug)\n" +
                        "MERGE (nBridget)-[:FRIEND]->(nMichael)\n" +
                        "MERGE (nCharles)-[:FRIEND]->(nMark)\n" +
                        "MERGE (nAlice)-[:FRIEND]->(nMichael)\n" +
                        "MERGE (nCharles)-[:FRIEND]->(nDoug)\n" +
                        "MERGE (nMark)-[:FRIEND]->(nKarin)\n" +
                        "MERGE (nKarin)-[:FRIEND]->(nAmy)\n" +
                        "MERGE (nAmy)-[:FRIEND]->(nDoug);";

        DB.resolveDependency(Procedures.class).registerProcedure(LouvainProc.class);
        DB.execute(cypher);
    }

    @Rule
    public ExpectedException exceptions = ExpectedException.none();

    @Test
    public void testStream() {
        final String cypher = "CALL algo.louvain.stream('', '', {concurrency:1, community: 'community'}) " +
                "YIELD nodeId, community, communities";
        final IntIntScatterMap testMap = new IntIntScatterMap();
        DB.execute(cypher).accept(row -> {
            final long nodeId = (long) row.get("nodeId");
            final long community = (long) row.get("community");
            System.out.println(nodeId + ": " + community);
            testMap.addTo((int) community, 1);
            return false;
        });
//        assertEquals(3, testMap.size());
    }




}
