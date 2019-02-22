package org.neo4j.graphalgo.similarity;

import java.util.concurrent.atomic.LongAdder;

public class SimilarityRecorder<T> implements SimilarityProc.SimilarityComputer<T>, Computations {

    private final SimilarityProc.SimilarityComputer computer;
    private final LongAdder computations = new LongAdder();

    public SimilarityRecorder(SimilarityProc.SimilarityComputer computer) {
        this.computer = computer;
    }

    public long count() {
        return computations.longValue();
    }


    @Override
    public SimilarityResult similarity(RleDecoder decoder, T source, T target, double cutoff) {
        computations.increment();
        return computer.similarity(decoder, source, target, cutoff);
    }
}
