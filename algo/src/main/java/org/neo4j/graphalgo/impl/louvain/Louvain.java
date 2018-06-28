package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Modularity based clustering algorithm. Uses {@link ModularityOptimization} algorithm
 * to perform optimization. if a better community structure has been found it is taken as
 * input for the next round. the algorithm creates a virtual graph based on the previous
 * community structure.
 *
 * @author mknblch
 */
public class Louvain extends Algorithm<Louvain> {

    private final int rootNodeCount;
    private int level;
    private final ExecutorService pool;
    private final int concurrency;
    private final AllocationTracker tracker;
    private ProgressLogger progressLogger;
    private TerminationFlag terminationFlag;
    private int[] communities;
    private int[][] dendogram;
    private Graph root;
    private int communityCount = 0;

    public Louvain(Graph graph,
                   ExecutorService pool,
                   int concurrency,
                   AllocationTracker tracker) {
        this.root = graph;
        this.pool = pool;
        this.concurrency = concurrency;
        this.tracker = tracker;
        rootNodeCount = Math.toIntExact(graph.nodeCount());
        communities = new int[rootNodeCount];
        tracker.add(4 * rootNodeCount);
        communityCount = rootNodeCount;
        Arrays.setAll(communities, i -> i);
    }

    public Louvain compute(int maxLevel, int maxIterations) {
        // temporary graph
        Graph graph = this.root;
        // result arrays
        dendogram = new int[maxLevel][];
        int cc = rootNodeCount;
        for (level = 0; level < maxLevel; level++) {
            // start modularity opzimization
            final ModularityOptimization modularityOptimization =
                    new ModularityOptimization(graph, pool, concurrency, tracker)
                            .withProgressLogger(progressLogger)
                            .withTerminationFlag(terminationFlag)
                            .compute(maxIterations);
            // rebuild graph based on the community structure
            final int[] communityIds = modularityOptimization.getCommunityIds();
            communityCount = ModularityOptimization.normalize(communityIds);
            // release the old algo instance
            modularityOptimization.release();
            progressLogger.log(
                    "level: " + (level + 1) +
                            " communities: " + communityCount +
                            " q: " + modularityOptimization.getModularity());
            if (communityCount == cc) {
                break;
            }
            cc = communityCount;
            dendogram[level] = rebuildCommunityStructure(communityIds);
            graph = rebuild(graph, communityIds);
        }
        return this;
    }

    /**
     * create a virtual graph based on the community structure of the
     * previous louvain round
     *
     * @param graph previous graph
     * @param communityIds community structure
     * @return a new graph built from a community structure
     */
    private Graph rebuild(Graph graph, int[] communityIds) {

        // count and normalize community structure
        final int nodeCount = communityIds.length;
        // bag of nodeId->{nodeId, ..}
        final IntObjectMap<IntScatterSet> relationships = new IntObjectScatterMap<>(nodeCount);
        // accumulated weights
        final LongDoubleScatterMap weights = new LongDoubleScatterMap(nodeCount);
        // for each node in the current graph
        for (int i = 0; i < nodeCount; i++) {
            // map node nodeId to community nodeId
            final int source = communityIds[i];
            // get transitions from current node
            graph.forEachRelationship(i, Direction.OUTGOING, (s, t, r) -> {
                // mapping
                final int target = communityIds[t];

                // omit self loops
                if (source == target) {
                    return true;
                }

                final double value = graph.weightOf(s, t);
                // add IN and OUT relation
                // aggregate weights
                if (find(relationships, source).add(target)) {
                    weights.addTo(RawValues.combineIntInt(source, target), value);
                }
                if (find(relationships, target).add(source)) {
                    weights.addTo(RawValues.combineIntInt(target, source), value);
                }
                return true;
            });
        }

        // create temporary graph
        return new LouvainGraph(communityCount, relationships, weights);
    }

    private int[] rebuildCommunityStructure(int[] communityIds) {
        // rebuild community array
        final int[] ints = new int[rootNodeCount];
        Arrays.setAll(ints, i -> communityIds[communities[i]]);
        communities = ints;
        return communities;
    }

    /**
     * nodeId to community mapping array
     * @return
     */
    public int[] getCommunityIds() {
        return communities;
    }

    public int[] getCommunityIds(int level) {
        return dendogram[level];
    }

    public int[][] getDendogram() {
        return dendogram;
    }

    /**
     * number of outer iterations
     * @return
     */
    public int getLevel() {
        return level;
    }

    /**
     * number of distinct communities
     * @return
     */
    public long getCommunityCount() {
        return communityCount;
    }

    /**
     * result stream
     * @return
     */
    public Stream<Result> resultStream() {
        return IntStream.range(0, rootNodeCount)
                .mapToObj(i -> new Result(i, communities[i]));
    }

    @Override
    public Louvain me() {
        return this;
    }

    @Override
    public Louvain release() {
        tracker.add(4 * rootNodeCount);
        communities = null;
        return this;
    }

    @Override
    public Louvain withProgressLogger(ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
        return this;
    }

    @Override
    public Louvain withTerminationFlag(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
        return this;
    }

    private static IntScatterSet find(IntObjectMap<IntScatterSet> relationships, int n) {
        final IntScatterSet intCursors = relationships.get(n);
        if (null == intCursors) {
            final IntScatterSet newList = new IntScatterSet();
            relationships.put(n, newList);
            return newList;
        }
        return intCursors;
    }

    /**
     *
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
