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
package org.neo4j.graphalgo.algo;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.InfoMapProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.impl.pagerank.PageRankResult;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

/**
 * Graph:
 *
 *        (b)        (e)
 *       /  \       /  \     (x)
 *     (a)--(c)---(d)--(f)
 *
 * @author mknblch
 */
public class InfoMapIntTest {

    private static final String QUERY = "CALL algo.infoMap.stream('Node', 'TYPE', {pr_iterations:10}) YIELD nodeId, community";

    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    private static Graph graph;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a', pr:1.0}})\n" +
                        "CREATE (b:Node {name:'b', pr:1.0})\n" +
                        "CREATE (c:Node {name:'c', pr:2.0})\n" +
                        "CREATE (d:Node {name:'d', pr:2.0})\n" +
                        "CREATE (e:Node {name:'e', pr:1.0})\n" +
                        "CREATE (f:Node {name:'f', pr:1.0})\n" +
                        "CREATE (x:Node {name:'x', pr:1.0})\n" +
                        "CREATE" +
                        " (b)-[:TYPE {v:1.0}]->(a),\n" +
                        " (a)-[:TYPE {v:1.0}]->(c),\n" +
                        " (c)-[:TYPE {v:1.0}]->(a),\n" +

                        " (d)-[:TYPE {v:1.0}]->(c),\n" +

                        " (d)-[:TYPE {v:1.0}]->(e),\n" +
                        " (d)-[:TYPE {v:1.0}]->(f),\n" +
                        " (e)-[:TYPE {v:1.0}]->(f)";

        db.execute(cypher);
        db.resolveDependency(Procedures.class).registerProcedure(InfoMapProc.class);
    }

    @Test
    public void testUnweightedStream() throws Exception {


        db.execute(QUERY).accept(row -> {
            System.out.printf("node %d | community %d%n",
                    row.getNumber("nodeId").intValue(),
                    row.getNumber("community").intValue());

            return true;
        });
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
