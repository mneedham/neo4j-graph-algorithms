package org.neo4j.graphalgo.results;

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.sorting.IndirectComparator;
import com.carrotsearch.hppc.sorting.IndirectSort;
import org.HdrHistogram.Histogram;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntToLongFunction;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

/**
 * @author mknblch
 */
public abstract class CommunityResult {

    /*

    YIELD loadMillis, computeMillis, writeMillis, nodes, communityCount, iterations, convergence, p99, p95, p90, p75, p50, top3

     */

    public final long loadMillis;
    public final long computeMillis;
    public final long writeMillis;
    public final long nodes;
    public final long communityCount;
    public final long iterations;
    public final boolean convergence;
    public final long p99;
    public final long p95;
    public final long p90;
    public final long p75;
    public final long p50;
    public final long[] top3;

    CommunityResult(long loadMillis,
                    long computeMillis,
                    long writeMillis,
                    long nodes,
                    long communityCount,
                    long iterations,
                    boolean convergence,
                    long p99,
                    long p95,
                    long p90,
                    long p75,
                    long p50,
                    long[] biggestCommunities) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.nodes = nodes;
        this.communityCount = communityCount;
        this.iterations = iterations;
        this.convergence = convergence;
        this.p99 = p99;
        this.p95 = p95;
        this.p90 = p90;
        this.p75 = p75;
        this.p50 = p50;
        this.top3 = biggestCommunities;
    }

    public static abstract class AbstractCommunityBuilder<T extends CommunityResult> extends AbstractResultBuilder<T> {

        protected long nodes = 1;
        protected long iterations = -1;
        protected boolean convergence = false;
        protected long communityCount = -1;
        protected final Histogram histogram = new Histogram(2);
        protected final LongLongHashMap communityMap = new LongLongHashMap();
        protected final long[] top3 = new long[]{-1L, -1L, -1L};


        public AbstractCommunityBuilder<T> withIterations(long iterations) {
            this.iterations = iterations;
            return this;
        }

        public AbstractCommunityBuilder<T> withConvergence(boolean convergence) {
            this.convergence = convergence;
            return this;
        }

        public AbstractCommunityBuilder<T> withCommunities(long nodes, LongFunction<Long> fun) {

            top3[0] = -1L;
            top3[1] = -1L;
            top3[2] = -1L;

            this.nodes = nodes;

            for (int i = 0; i < nodes; i++) {
                // map to community id
                final long r = fun.apply(i);
                // aggregate
                communityMap.addTo(r, 1);
                // create histogram
                histogram.recordValue(r);
                // eval top 3 communities
                if (r > top3[0]) {
                    top3[2] = top3[1];
                    top3[1] = top3[0];
                    top3[0] = r;
                } else if (r > top3[1]) {
                    top3[2] = top3[1];
                    top3[1] = r;
                } else if (r > top3[2]) {
                    top3[2] = r;
                }
            }

            return this;
        }

        public abstract T build();

    }
}
