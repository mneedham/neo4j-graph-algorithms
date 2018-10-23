package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.infomap.MapEquation;
import org.neo4j.graphalgo.impl.pagerank.PageRankAlgorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class MapEquationProc {

    @Context
    public GraphDatabaseAPI db;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.mapEquation.stream")
    @Description("...")
    public Stream<Result> mapEquation(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipType,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> configuration) {

        final ProcedureConfiguration config = ProcedureConfiguration.create(configuration)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationshipType);

        final Graph graph = new GraphLoader(db, Pools.DEFAULT)
                .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                .load(config.getGraphImpl());


        final PageRankResult pageRankResult = PageRankAlgorithm.of(graph, 1. - MapEquation.TAU, LongStream.empty())
                .compute(config.get("pr_iterations", 10))
                .result();

        // TODO: normalize based on sum of weights of a node
        final RelationshipWeights normalizedWeights = (sourceNodeId, targetNodeId) -> {
            final int degree = graph.degree(sourceNodeId, Direction.OUTGOING);
            if (degree == 0) {
                return 0;
            }
            return 1. / degree;
        };

        final int[] communities = new MapEquation(graph, pageRankResult::score, normalizedWeights)
                .compute(config.getIterations(10), config.get("shuffle", true))
                .getCommunities();

        return IntStream.range(0, Math.toIntExact(graph.nodeCount()))
                .mapToObj(i -> new Result(graph.toOriginalNodeId(i), communities[i]));
    }

    /**
     * result object
     */
    public static final class Result {

        public final long nodeId;
        public final long community;

        public Result(long id, long community) {
            this.nodeId = id;
            this.community = community;
        }
    }
}
