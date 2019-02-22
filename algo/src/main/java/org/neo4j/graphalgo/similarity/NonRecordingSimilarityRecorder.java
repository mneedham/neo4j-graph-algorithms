package org.neo4j.graphalgo.similarity;

public class NonRecordingSimilarityRecorder<T> implements SimilarityRecorder<T> {
    private final SimilarityProc.SimilarityComputer computer;

    public NonRecordingSimilarityRecorder(SimilarityProc.SimilarityComputer computer) {
        this.computer = computer;
    }

    public long count() {
        return -1;
    }


    @Override
    public SimilarityResult similarity(RleDecoder decoder, T source, T target, double cutoff) {
        return computer.similarity(decoder, source, target, cutoff);
    }
}


interface SimilarityRecorder<T> extends Computations, SimilarityProc.SimilarityComputer<T> {
}