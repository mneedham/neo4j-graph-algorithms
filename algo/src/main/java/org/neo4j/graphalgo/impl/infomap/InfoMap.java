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
package org.neo4j.graphalgo.impl.infomap;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntDoubleCursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.utils.*;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
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

    public static int PAGE_RANK_BATCH_SIZE = 10_000;
    public static boolean PAGE_RANK_CACHE_WEIGHTS = false;
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
    // normalized relationship weights
    private RelationshipWeights weights;
    // helper vars
    private final double tau1, n1;

    // default env
    private final ForkJoinPool pool;
    private final int concurrency;
    private final ProgressLogger logger;
    private final TerminationFlag terminationFlag;

    // following values are updated during a merge
    // number of iterations the last computation took
    private int iterations = 0;
    // module map
    private IndexMap<Module> modules;
    // community assignment helper array
    private int[] communities;
    // sum of exit probs.
    private double sQi;

    /**
     * create a weighted InfoMap algo instance
     */
    public static InfoMap weighted(Graph graph, int prIterations, RelationshipWeights weights, double threshold, double tau, ForkJoinPool pool, int concurrency, ProgressLogger logger, TerminationFlag terminationFlag) {

        final PageRankResult pageRankResult;
        // use parallel PR if concurrency is >1
        if (concurrency > 1) {
            pageRankResult = PageRankAlgorithm.weightedOf(AllocationTracker.create(), graph, 1. - tau, LongStream.empty(), pool, concurrency, PAGE_RANK_BATCH_SIZE, PAGE_RANK_CACHE_WEIGHTS)
                    .compute(prIterations)
                    .result();
            return weighted(graph, pageRankResult::score, weights, threshold, tau, pool, concurrency, logger, terminationFlag);
        } else {
            pageRankResult = PageRankAlgorithm.weightedOf(graph, 1. - tau, LongStream.empty())
                    .compute(prIterations)
                    .result();
        }

        return weighted(graph, pageRankResult::score, weights, threshold, tau, pool, concurrency, logger, terminationFlag);
    }

    /**
     * create a weighted InfoMap algo instance with pageRanks
     */
    public static InfoMap weighted(Graph graph, NodeWeights pageRanks, RelationshipWeights weights, double threshold, double tau, ForkJoinPool pool, int concurrency, ProgressLogger logger, TerminationFlag terminationFlag) {
        return new InfoMap(
                graph,
                pageRanks,
                new NormalizedRelationshipWeights(Math.toIntExact(graph.nodeCount()), graph, weights),
                threshold,
                tau, pool, concurrency, logger, terminationFlag);
    }

    /**
     * create an unweighted InfoMap algo instance
     */
    public static InfoMap unweighted(Graph graph, int prIterations, double threshold, double tau, ForkJoinPool pool, int concurrency, ProgressLogger logger, TerminationFlag terminationFlag) {
        final PageRankResult pageRankResult;

        // use parallel PR if concurrency is >1
        if (concurrency > 1) {
            final AllocationTracker tracker = AllocationTracker.create();
            pageRankResult = PageRankAlgorithm.of(tracker, graph, 1. - tau, LongStream.empty(), pool, concurrency, PAGE_RANK_BATCH_SIZE)
                    .compute(prIterations)
                    .result();
        } else {
            pageRankResult = PageRankAlgorithm.of(graph, 1. - tau, LongStream.empty())
                    .compute(prIterations)
                    .result();
        }
        return unweighted(graph, pageRankResult::score, threshold, tau, pool, concurrency, logger, terminationFlag);
    }

    /**
     * create an unweighted InfoMap algo instance
     */
    public static InfoMap unweighted(Graph graph, NodeWeights pageRanks, double threshold, double tau, ForkJoinPool pool, int concurrency, ProgressLogger logger, TerminationFlag terminationFlag) {
        return new InfoMap(
                graph,
                pageRanks,
                new DegreeNormalizedRelationshipWeights(graph),
                threshold,
                tau, pool, concurrency, logger, terminationFlag);
    }

    /**
     * @param graph             graph
     * @param pageRank          page ranks
     * @param normalizedWeights normalized weights (weights of a node must sum up to 1.0)
     * @param threshold         minimum delta L for optimization
     * @param tau               constant tau (usually 0.15)
     * @param pool              executor service
     * @param concurrency       number of threads
     * @param logger            log
     * @param terminationFlag   running flag
     */
    private InfoMap(Graph graph, NodeWeights pageRank, RelationshipWeights normalizedWeights, double threshold, double tau, ForkJoinPool pool, int concurrency, ProgressLogger logger, TerminationFlag terminationFlag) {
        this.weights = normalizedWeights;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.tau = tau;
        this.threshold = threshold;
        this.pool = pool;
        this.concurrency = concurrency;
        this.logger = logger;
        this.terminationFlag = terminationFlag;
        this.modules = new IndexMap<>(MODULE_POS, Module.class, nodeCount);
        this.communities = new int[nodeCount];
        this.tau1 = 1. - tau;
        this.n1 = nodeCount - 1.;
        this.sQi = 0.0;
        Arrays.setAll(communities, i -> i);
        graph.forEachNode(node -> {
            Module module = new Module(node, graph, pageRank);
            modules.put(node, module);
            sQi += module.q;
            return true;
        });
    }

    /**
     * minimize description length
     */
    public InfoMap compute() {
        this.iterations = 0;
        long ts = System.currentTimeMillis();
        while (terminationFlag.running() && optimize()) {
            this.iterations++;
            final long start = ts;
            final long c = System.currentTimeMillis();
            logger.logProgress(iterations / (double) nodeCount, () -> "Iteration " + iterations + " took " + (c - start) + "ms");
            ts = c;
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
        modules.forEach(Module::release);
        modules.release();
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

        final MergePair pair = pool.invoke(new Task(modules.array(), 0, modules.size(), new BitSet()));

        if (null == pair) {
            return false;
        }

        pair.modA.merge(pair.modB);
        this.modules.remove(pair.modB.index);
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

        private final Module[] m;
        private final int from;
        private final int to;
        private final BitSet visited;

        private Task(Module[] m, int from, int to, BitSet visited) {
            this.m = m;
            this.from = from;
            this.to = to;
            this.visited = visited;
        }

        @Override
        protected MergePair compute() {

            int length = to - from;
            if (concurrency > 1 && length >= MIN_MODS_PARALLEL_EXEC) {
                // split mods
                final int half = from + ((length + 1) >> 1);
                final Task taskA = new Task(m, from, half, null);
                taskA.fork();
                final Task taskB = new Task(m, half, to, visited);
                final MergePair mpA = taskB.compute();
                final MergePair mpB = taskA.join();
                return compare(mpA, mpB);
            }

            final Pointer.DoublePointer min = Pointer.wrap(-1 * threshold);

            final Module[] best = {null, null};
            BitSet visited = this.visited;
            if (visited == null) {
                visited = new BitSet();
            }
            for (int i = from; i < to; i++) {
                final Module module = m[i];
                module.forEachNeighbor(l -> {
                    final double v = delta(module, l);
                    if (v < min.v) {
                        min.v = v;
                        best[0] = module;
                        best[1] = l;
                    }
                }, visited, modules);
            }

            if (null == best[0] || best[0] == best[1]) {
                return null;
            }

            return new MergePair(best[0], best[1], min.v);

        }
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
        // position from where this was deleted from `Modules`
        int initialPosition = -1;
        // module id (first node in the set)
        final int index;
        // set size (number of nodes)
        int n = 1;
        // ergodic frequency
        double p;
        // exit probability without teleport
        double w = .0;
        // exit probability with teleport
        double q;
        // nodes
        private BitSet nodes;
        // precalculated weights into other communities
        private IntDoubleMap wi;

        Module(int startNode, Graph graph, NodeWeights pageRank) {
            this.index = startNode;
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

        void forEachNeighbor(
                Consumer<Module> consumer,
                BitSet visited,
                IndexMap<Module> modules) {
            visited.clear();
            for (final IntDoubleCursor cursor : this.wi) {
                final int node = cursor.key;
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
            }
        }

        double wil(Module l) {
            double wi = 0.;
            for (final IntDoubleCursor cursor : this.wi) {
                if (communities[cursor.key] == l.index) {
                    wi += cursor.value;
                }
            }
            return wi;
        }

        void merge(Module l) {
            n += l.n;
            p += l.p;
            w += l.w - wil(l);
            if (nodes == null) {
                nodes = new BitSet(index);
                nodes.set(index);
            }
            if (l.nodes != null) {
                nodes.or(l.nodes);
                BitSetIterator iterator = l.nodes.iterator();
                int lNode;
                while ((lNode = iterator.nextSetBit()) != BitSetIterator.NO_MORE) {
                    communities[lNode] = index;
                }
            } else {
                nodes.set(l.index);
                communities[l.index] = index;
            }
            wi.putAll(l.wi);
            wi.removeAll(nodes.asIntLookupContainer());
            sQi -= q + l.q;
            q = tau * p * (nodeCount - n) / n1 + tau1 * w;
            sQi += q;
            l.release();
        }

        void release() {
            wi = null;
            nodes = null;
        }
    }

    private static MergePair compare(MergePair mpA, MergePair mpB) {
        if (null == mpA && null == mpB) {
            return null;
        }
        if (null == mpA) {
            return mpB;
        }
        if (null == mpB) {
            return mpA;
        }
        return mpA.deltaL < mpB.deltaL ? mpA : mpB;
    }

    private static double plogp(double v) {
        return v > .0 ? v * log2(v) : 0.;
    }

    private static double log2(double v) {
        return Math.log(v) / LOG2;
    }

    private static final IndexMap.PositionMarker<Module> MODULE_POS = new IndexMap.PositionMarker<Module>() {
        @Override
        public int position(final Module item) {
            return item.initialPosition;
        }

        @Override
        public void setPosition(final Module item, final int position) {
            item.initialPosition = position;
        }
    };
}
