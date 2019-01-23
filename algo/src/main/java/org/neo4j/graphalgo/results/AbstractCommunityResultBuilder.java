package org.neo4j.graphalgo.results;

import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.LongLongScatterMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.HdrHistogram.Histogram;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.ProgressTimer;

import java.util.Arrays;
import java.util.List;
import java.util.function.LongFunction;

/**
 * @author mknblch
 */
public abstract class AbstractCommunityResultBuilder<T> {

    protected long loadDuration = -1;
    protected long evalDuration = -1;
    protected long writeDuration = -1;

    public AbstractCommunityResultBuilder<T> withLoadDuration(long loadDuration) {

        this.loadDuration = loadDuration;
        return this;
    }


    public AbstractCommunityResultBuilder<T> withEvalDuration(long evalDuration) {

        this.evalDuration = evalDuration;
        return this;
    }


    public AbstractCommunityResultBuilder<T> withWriteDuration(long writeDuration) {

        this.writeDuration = writeDuration;
        return this;
    }

    /**
     * returns an AutoClosable which measures the time
     * until it gets closed. Saves the duration as loadMillis
     *
     * @return
     */
    public ProgressTimer timeLoad() {

        return ProgressTimer.start(this::withLoadDuration);
    }

    /**
     * returns an AutoClosable which measures the time
     * until it gets closed. Saves the duration as evalMillis
     *
     * @return
     */
    public ProgressTimer timeEval() {

        return ProgressTimer.start(this::withEvalDuration);
    }

    /**
     * returns an AutoClosable which measures the time
     * until it gets closed. Saves the duration as writeMillis
     *
     * @return
     */
    public ProgressTimer timeWrite() {

        return ProgressTimer.start(this::withWriteDuration);
    }

    /**
     * evaluates loadMillis
     *
     * @param runnable
     */
    public void timeLoad(Runnable runnable) {

        try (ProgressTimer timer = timeLoad()) {
            runnable.run();
        }
    }

    /**
     * evaluates comuteMillis
     *
     * @param runnable
     */
    public void timeEval(Runnable runnable) {

        try (ProgressTimer timer = timeEval()) {
            runnable.run();
        }
    }

    /**
     * evaluates writeMillis
     *
     * @param runnable
     */
    public void timeWrite(Runnable runnable) {

        try (ProgressTimer timer = timeWrite()) {
            runnable.run();
        }
    }

    /**
     * build result
     *
     * @param nodes number of nodes in the graph
     * @param fun   nodeId to communityId mapping function
     * @return result
     */
    public T build(IdMapping nodes, LongFunction<Long> fun) {

        final Histogram histogram = new Histogram(2);
        final LongLongMap communityMap = new LongLongScatterMap();
        final long nodeCount = nodes.nodeCount();
        final ProgressTimer timer = ProgressTimer.start();
        for (int i = 0; i < nodeCount; i++) {
            // map to community id
            final long r = fun.apply(i);
            // aggregate community size
            communityMap.addTo(r, 1);
            // fill histogram
            histogram.recordValue(r);
        }
        final List<Long> top3Communities = top3(nodes, communityMap);
        timer.stop();

        return build(loadDuration,
                evalDuration,
                writeDuration,
                timer.getDuration(),
                nodeCount,
                communityMap.size(),
                communityMap,
                histogram,
                top3Communities);
    }

    protected abstract T build(
            long loadMillis,
            long computeMillis,
            long writeMillis,
            long postProcessingMillis,
            long nodeCount,
            long communityCount,
            LongLongMap communitySizeMap,
            Histogram communityHistogram,
            List<Long> top3Communities);

    private List<Long> top3(IdMapping idMapping, LongLongMap assignment) {
        // index of top 3 biggest communities
        final Long[] t3idx = new Long[]{-1L, -1L, -1L};
        // size of the top 3 communities
        final long[] top3 = new long[]{-1L, -1L, -1L};
        for (LongLongCursor cursor : assignment) {
            if (cursor.value > top3[0]) {
                top3[2] = top3[1];
                top3[1] = top3[0];
                top3[0] = cursor.value;
                t3idx[2] = t3idx[1];
                t3idx[1] = t3idx[0];
                t3idx[0] = cursor.key;
            } else if (cursor.value > top3[1]) {
                top3[2] = top3[1];
                top3[1] = cursor.value;
                t3idx[2] = t3idx[1];
                t3idx[1] = cursor.key;
            } else if (cursor.value > top3[2]) {
                top3[2] = cursor.value;
                t3idx[2] = cursor.key;
            }
        }
        return Arrays.asList(t3idx);
    }
}
