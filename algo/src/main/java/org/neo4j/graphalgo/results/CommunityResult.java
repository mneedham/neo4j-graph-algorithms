package org.neo4j.graphalgo.results;

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.sorting.IndirectComparator;
import com.carrotsearch.hppc.sorting.IndirectSort;
import org.HdrHistogram.Histogram;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    public final List<Long> top3;

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
                    int[] biggestCommunities) {
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
        this.top3 = Arrays.stream(biggestCommunities).asLongStream().boxed().collect(Collectors.toList());
    }

    public static abstract class AbstractCommunityBuilder<T extends CommunityResult> extends AbstractResultBuilder<T> {

        protected long nodes = 1;
        protected long iterations = -1;
        protected boolean convergence = false;
        protected long communityCount = -1;
        protected final Histogram histogram = new Histogram(2);
        protected final IntIntHashMap communityMap = new IntIntHashMap();

        public AbstractCommunityBuilder withIterations(long iterations) {
            this.iterations = iterations;
            return this;
        }

        public AbstractCommunityBuilder withConvergence(boolean convergence) {
            this.convergence = convergence;
            return this;
        }

        public AbstractCommunityBuilder withNodes(long nodes) {
            this.nodes = nodes;
            return this;
        }

        public AbstractCommunityBuilder withCommunities(int[] communities) {
            for (int i = 0; i < communities.length; i++) {
                communityMap.addTo(communities[i], 1);
                histogram.recordValue(communities[i]);
            }
            return this;
        }

        // overwrite community count
        public AbstractCommunityBuilder withCommunityCount(long communityCount) {
            this.communityCount = communityCount;
            return this;
        }

        public AbstractCommunityBuilder withoutCommunities() {
            return this;
        }

        public static List<Long> top3(long elements, ToLongFunction<Long> fun) {

            long t1 = -1L, t2 = -1L, t3 = -1L;

            for (long i = 0; i < elements; i++) {
                final long r = fun.applyAsLong(i);
                if (r > t1) {
                    t3 = t2;
                    t2 = t1;
                    t1 = r;
                    continue;
                }
                if (r > t2) {
                    t3 = t2;
                    t2 = r;
                    continue;
                }
                if (r > t3) {
                    t3 = r;
                }
            }

            final ArrayList<Long> longs = new ArrayList<>();
            longs.add(t1);
            longs.add(t2);
            longs.add(t3);
            return longs;
        }

        public abstract T build();

    }

}
