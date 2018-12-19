package org.neo4j.graphalgo.impl.infomap;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.procedures.IntDoubleProcedure;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.utils.DegreeNormalizedRelationshipWeights;
import org.neo4j.graphalgo.core.utils.NormalizedRelationshipWeights;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankAlgorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankResult;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.function.Consumer;
import java.util.stream.LongStream;

/**
 * Java adaption of InfoMap from https://github.com/felixfung/InfoFlow
 *
 * @author mknblch, Mark Schaarschmidt
 */
public class InfoMap extends Algorithm<InfoMap> {

    private static final double LOG2 = Math.log(2.);

    public static int MIN_MODS_PARALLEL_EXEC = 20;
    // default TAU
    public static final double TAU = 0.15;
    // default threshold
    public static final double THRESHOLD = .005;

    // iteration direction is constant (undirected graph)
    private static final Direction D = Direction.OUTGOING;
    // nodes in the graph
    private final int nodeCount;
    // constant TAU (0.15 was given in the original MapEq. paper https://arxiv.org/abs/0906.1405 )
    private final double tau;
    // minimum difference in deltaL for merging 2 modules together
    private final double threshold;
    // an undirected graph
    private Graph graph;
    // page rank weights
    private NodeWeights pageRank;
    // normalized relationship weights
    private RelationshipWeights weights;
    // helper vars
    private final double tau1, n1;

    private final ForkJoinPool pool;
    private final int concurrency;

    // following values are updated during a merge
    // number of iterations the last computation took
    private int iterations = 0;
    // module map
    private IntObjectMap<Module> modules;
    // community assignment helper array
    private int[] communities;
    // sum of exit probs.
    private double sQi = 0.;

    /**
     * create a weighted InfoMap algo instance
     */
    public static InfoMap weighted(Graph graph, int prIterations, RelationshipWeights weights, double threshold, double tau, ForkJoinPool pool, int concurrency) {
        final PageRankResult pageRankResult = PageRankAlgorithm.of(graph, 1. - tau, LongStream.empty())
                .compute(prIterations)
                .result();
        return weighted(graph, pageRankResult::score, weights, threshold, tau, pool, concurrency);
    }

    /**
     * create a weighted InfoMap algo instance with pageRanks
     */
    public static InfoMap weighted(Graph graph, NodeWeights pageRanks, RelationshipWeights weights, double threshold, double tau, ForkJoinPool pool, int concurrency) {
        return new InfoMap(
                graph,
                pageRanks,
                new NormalizedRelationshipWeights(Math.toIntExact(graph.nodeCount()), graph, weights),
                threshold,
                tau, pool, concurrency);
    }

    /**
     * create an unweighted InfoMap algo instance
     */
    public static InfoMap unweighted(Graph graph, int prIterations, double threshold, double tau, ForkJoinPool pool, int concurrency) {
        final PageRankResult pageRankResult = PageRankAlgorithm.of(graph, 1. - tau, LongStream.empty())
                .compute(prIterations)
                .result();
        return unweighted(graph, pageRankResult::score, threshold, tau, pool, concurrency);
    }

    /**
     * create an unweighted InfoMap algo instance
     */
    public static InfoMap unweighted(Graph graph, NodeWeights pageRanks, double threshold, double tau, ForkJoinPool pool, int concurrency) {
        return new InfoMap(
                graph,
                pageRanks,
                new DegreeNormalizedRelationshipWeights(graph),
                threshold,
                tau, pool, concurrency);
    }

    /**
     * @param graph             graph
     * @param pageRank          page ranks
     * @param normalizedWeights normalized weights (weights of a node must sum up to 1.0)
     * @param threshold         minimum delta L for optimization
     * @param tau               constant tau (usually 0.15)
     * @param pool
     * @param concurrency
     */
    private InfoMap(Graph graph, NodeWeights pageRank, RelationshipWeights normalizedWeights, double threshold, double tau, ForkJoinPool pool, int concurrency) {
        this.graph = graph;
        this.pageRank = pageRank;
        this.weights = normalizedWeights;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.tau = tau;
        this.threshold = threshold;
        this.pool = pool;
        this.concurrency = concurrency;
        this.modules = new IntObjectScatterMap<>(nodeCount);
        this.communities = new int[nodeCount];
        this.tau1 = 1. - tau;
        this.n1 = nodeCount - 1.;
        Arrays.setAll(communities, i -> i);
        final double[] d = {0.};
        graph.forEachNode(node -> {
            final Module module = new Module(node);
            modules.put(node, module);
            d[0] += module.q;
            return true;
        });
        this.sQi = d[0];
    }

    /**
     * minimize description length
     */
    public InfoMap compute() {
        this.iterations = 0;
        while (optimize()) {
            this.iterations++;
        }
        return this;
    }

    @Override
    public InfoMap me() {
        return this;
    }

    /**
     * release data
     *
     * @return
     */
    @Override
    public InfoMap release() {
        graph = null;
        pageRank = null;
        weights = null;
        for (IntObjectCursor<Module> cursor : modules) {
            cursor.value.release();
        }
        modules = null;
        communities = null;
        return this;
    }

    /**
     * return community array
     *
     * @return
     */
    public int[] getCommunities() {
        return communities;
    }

    /**
     * number of distinct modules left
     *
     * @return number of modules
     */
    public int getCommunityCount() {
        return modules.size();
    }

    /**
     * return number of iterations
     *
     * @return number of iterations
     */
    public int getIterations() {
        return iterations;
    }

    /**
     * find a pair of modules that lead to the
     * best reduction in L and merge them as
     * long as their absolute difference is
     * higher then {@link InfoMap#threshold}
     *
     * @return true if a merge occurred, false otherwise
     */
    private boolean optimize() {

        final MergePair pair = pool.invoke(new Task(this.modules.values().toArray(Module.class)));

        if (null == pair) {
            return false;
        }

        pair.modA.merge(pair.modB);
        modules.remove(pair.modB.index);
        return true;
    }

    /**
     * change in L if module j and k are merged
     *
     * @return delta L
     */
    private double delta(Module j, Module k) {
        double pi = j.p + k.p;
        double qi = tau * pi * (nodeCount - ((double) (j.n + k.n))) / n1 + tau1 * (j.w + k.w - j.wil(k));
        return plogp(qi - j.q - k.q + sQi)
                - plogp(sQi)
                - 2 * plogp(qi)
                + 2 * plogp(j.q)
                + 2 * plogp(k.q)
                + plogp(pi + qi)
                - plogp(j.p + j.q)
                - plogp(k.p + k.q);
    }

    private class Task extends RecursiveTask<MergePair> {

        final Module[] m;

        private Task(Module[] m) {
            this.m = m;
        }

        @Override
        protected MergePair compute() {

            if (m.length >= MIN_MODS_PARALLEL_EXEC) {
                // split mods
                int half = m.length / 2;
                final Task taskA = new Task(Arrays.copyOfRange(m, 0, half));
                taskA.fork();
                final Task taskB = new Task(Arrays.copyOfRange(m, half, m.length));
                final MergePair mpA = taskB.compute();
                final MergePair mpB = taskA.join();
                return compare(mpA, mpB);
            }

            final Pointer.DoublePointer min = Pointer.wrap(-1 * threshold);

            final Module[] best = {null, null};
            for (Module module : m) {
                module.forEachNeighbor(l -> {
                    final double v = delta(module, l);
                    if (v < min.v) {
                        min.v = v;
                        best[0] = module;
                        best[1] = l;
                    }
                });
            }

            if (null == best[0] || best[0] == best[1]) {
                return null;
            }

            return new MergePair(best[0], best[1], min.v);

        }
    }

    private MergePair compare(MergePair mpA, MergePair mpB) {
        if (null == mpA && null == mpB) {
            return null;
        }
        if (null == mpB) {
            return mpA;
        }
        if (null == mpA) {
            return mpB;
        }

        return mpA.deltaL < mpB.deltaL ? mpA : mpB;
    }

    private static class MergePair {

        final Module modA;
        final Module modB;
        final double deltaL;

        private MergePair(Module modA, Module modB, double deltaL) {
            this.modA = modA;
            this.modB = modB;
            this.deltaL = deltaL;
        }
    }

    /**
     * a module represents a community
     */
    private class Module {
        // module id (first node in the set)
        final int index;
        // nodes
        BitSet nodes;
        // set size (number of nodes)
        int n = 1;
        // ergodic frequency
        double p;
        // exit probability without teleport
        double w = .0;
        // exit probability with teleport
        double q;
        // precalculated weights into other communities
        IntDoubleMap wi;
        // visited
        BitSet visited = new BitSet(nodeCount);

        Module(int startNode) {
            this.nodes = new BitSet(nodeCount);
            this.index = startNode;
            this.nodes.set(startNode);
            this.wi = new IntDoubleScatterMap();
            graph.forEachRelationship(startNode, D, (s, t, r) -> {
                if (s != t) {
                    final double v = weights.weightOf(s, t);
                    w += v;
                    wi.put(t, v * pageRank.weightOf(s) + weights.weightOf(t, s) * pageRank.weightOf(t));
                }
                return true;
            });
            p = pageRank.weightOf(startNode);
            w *= p;
            q = tau * p + tau1 * w;
        }

        void forEachNeighbor(Consumer<Module> consumer) {
            visited.clear();
            wi.keys().forEach((IntProcedure) node -> {
                final int c = communities[node];
                if (c == index) {
                    return;
                }
                // already visited
                if (visited.get(c)) {
                    return;
                }
                // do visit
                visited.set(c);
                consumer.accept(modules.get(c));
            });
        }

        double wil(Module l) {
            final Pointer.DoublePointer wi = Pointer.wrap(0.);
            this.wi.forEach((IntDoubleProcedure) (key, value) -> {
                if (communities[key] == l.index) {
                    wi.v += value;
                }
            });
            return wi.v;
        }

        void merge(Module l) {
            n += l.n;
            p += l.p;
            w += l.w - wil(l);
            nodes.or(l.nodes);
            wi.putAll(l.wi);
            wi.removeAll(nodes.asIntLookupContainer());
            sQi -= q + l.q;
            q = tau * p * (nodeCount - n) / n1 + tau1 * w;
            sQi += q;
            l.nodes.asIntLookupContainer().forEach((IntProcedure) n -> communities[n] = index);
            l.release();
        }

        void release() {
            wi = null;
            nodes = null;
            visited = null;
        }
    }

    private static double plogp(double v) {
        return v > .0 ? v * log2(v) : 0.;
    }

    private static double log2(double v) {
        return Math.log(v) / LOG2;
    }
}
