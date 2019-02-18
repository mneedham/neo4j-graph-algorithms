package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.impl.util.TopKConsumer;

import static org.neo4j.graphalgo.similarity.SimilarityProc.computeSimilarityForSourceIndex;

class TopKTask<T> implements Runnable {
    private SimilarityProc similarityProc;
    private final int batchSize;
    private final int taskOffset;
    private final int multiplier;
    private final int length;
    private final T[] ids;
    private final double similiarityCutoff;
    private final SimilarityProc.SimilarityComputer<T> computer;
    private RleDecoder decoder;
    private final TopKConsumer<SimilarityResult>[] topKConsumers;

    TopKTask(int batchSize, int taskOffset, int multiplier, int length, T[] ids, double similiarityCutoff, int topK, SimilarityProc.SimilarityComputer<T> computer, RleDecoder decoder) {
        this.batchSize = batchSize;
        this.taskOffset = taskOffset;
        this.multiplier = multiplier;
        this.length = length;
        this.ids = ids;
        this.similiarityCutoff = similiarityCutoff;
        this.computer = computer;
        this.decoder = decoder;
        topKConsumers = SimilarityProc.initializeTopKConsumers(length, topK);
    }

    @Override
    public void run() {
        SimilarityConsumer consumer = SimilarityProc.assignSimilarityPairs(topKConsumers);
        for (int offset = 0; offset < batchSize; offset++) {
            int sourceId = taskOffset * multiplier + offset;
            if (sourceId < length) {

                computeSimilarityForSourceIndex(sourceId, ids, length, similiarityCutoff, consumer, computer, decoder);
            }
        }
    }


    void mergeInto(TopKConsumer<SimilarityResult>[] target) {
        for (int i = 0; i < target.length; i++) {
            target[i].accept(topKConsumers[i]);
        }
    }
}
