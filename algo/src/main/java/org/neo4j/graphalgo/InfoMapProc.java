package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.infomap.InfoMap;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.slf4j.Logger;

import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author mknblch
 */
public class InfoMapProc {

    public static final String PAGE_RANK_PROPERTY = "pageRankProperty";

    @Context
    public GraphDatabaseAPI db;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;


    /*

    1. infomap stream, unweighted, with ext. pageRanks
    2. infomap stream, weighted, with ext. pageRanks
    3. infomap stream, unweighted, calc pr
    4. infomap stream, weighted, calc pr

    1. infomap writeback, unweighted, with ext. pageRanks
    2. infomap writeback, weighted, with ext. pageRanks
    3. infomap writeback, unweighted, calc pr
    4. infomap writeback, weighted, calc pr

    Options:

    - predefined weight-property name for pageRanks
        + if given ext. pageRank should be used
    - predefined weight-property for relationships
        + if given weight property on relationships is used

     */


    private enum Setup {
        WEIGTED, WEIGHTED_EXT_PR, UNWEIGHTED, UNWEIGHTED_EXT_PR
    }

    @Procedure("algo.infoMap.stream")
    @Description("...")
    public Stream<Result> stream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipType,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> configuration) {

        final ProcedureConfiguration config = ProcedureConfiguration.create(configuration)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationshipType);


        final Setup setup;
        if (config.hasWeightProperty()) {
            if (config.containsKeys(PAGE_RANK_PROPERTY)) setup = Setup.WEIGHTED_EXT_PR;
            else setup = Setup.WEIGTED;
        } else {
            if (config.containsKeys(PAGE_RANK_PROPERTY)) setup = Setup.UNWEIGHTED_EXT_PR;
            else setup = Setup.UNWEIGHTED;
        }

        final Graph graph;
        final InfoMap infoMap;


        // number of iterations for the pageRank computation
        final int pageRankIterations = config.getNumber("pr_iterations", 5).intValue();
        // property name (node property) for predefined pageRanks
        final String pageRankPropertyName = config.getString(PAGE_RANK_PROPERTY, "pageRank");

        switch (setup) {

            case WEIGTED:

                log.info("initializing weighted InfoMap with internal PageRank computation");
                graph = new GraphLoader(db, Pools.DEFAULT)
                        .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                        .withRelationshipWeightsFromProperty(config.getWeightProperty(), 1.0)
                        .asUndirected(true)
                        .load(config.getGraphImpl());
                infoMap = InfoMap.weighted(
                        graph,
                        pageRankIterations,
                        graph,
                        config.getNumber("tau", InfoMap.TAU).doubleValue(),
                        config.getNumber("threshold", InfoMap.THRESHOLD).doubleValue());
                break;

            case WEIGHTED_EXT_PR:

                log.info("initializing weighted InfoMap with predefined PageRank");
                graph = new GraphLoader(db, Pools.DEFAULT)
                        .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                        .withRelationshipWeightsFromProperty(config.getWeightProperty(), 1.0)
                        .withOptionalNodeProperties(PropertyMapping.of("_pr", pageRankPropertyName,0.))
                        .asUndirected(true)
                        .load(HeavyGraphFactory.class); // NodeProperties iface on Huge?
                infoMap = InfoMap.weighted(
                        graph,
                        ((NodeProperties) graph).nodeProperties("_pr")::get,
                        graph,
                        config.getNumber("tau", InfoMap.TAU).doubleValue(),
                        config.getNumber("threshold", InfoMap.THRESHOLD).doubleValue());
                break;

            case UNWEIGHTED:

                log.info("initializing unweighted InfoMap with internal PageRank computation");
                graph = new GraphLoader(db, Pools.DEFAULT)
                        .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                        .asUndirected(true)
                        .load(config.getGraphImpl());
                infoMap = InfoMap.unweighted(
                        graph,
                        pageRankIterations,
                        config.getNumber("tau", InfoMap.TAU).doubleValue(),
                        config.getNumber("threshold", InfoMap.THRESHOLD).doubleValue());
                break;

            case UNWEIGHTED_EXT_PR:

                log.info("initializing unweighted InfoMap with predefined PageRank");
                graph = new GraphLoader(db, Pools.DEFAULT)
                        .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                        .withOptionalNodeProperties(PropertyMapping.of("_pr", pageRankPropertyName, 0.))
                        .asUndirected(true)
                        .load(HeavyGraphFactory.class); // NodeProperties iface on Huge?
                infoMap = InfoMap.unweighted(
                        graph,
                        ((NodeProperties) graph).nodeProperties("_pr")::get,
                        config.getNumber("tau", InfoMap.TAU).doubleValue(),
                        config.getNumber("threshold", InfoMap.THRESHOLD).doubleValue());
                break;

            default:
                throw new IllegalArgumentException();
        }

        final int[] communities = infoMap.compute().getCommunities();
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
