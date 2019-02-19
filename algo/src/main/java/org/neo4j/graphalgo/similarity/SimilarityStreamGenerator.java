package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.QueueBasedSpliterator;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.util.TopKConsumer;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SimilarityStreamGenerator<T> {
    private final TerminationFlag terminationFlag;
    private final ProcedureConfiguration configuration;
    private final Supplier<RleDecoder> decoderFactory;
    private final SimilarityProc.SimilarityComputer<T> computer;

    public SimilarityStreamGenerator(TerminationFlag terminationFlag, ProcedureConfiguration configuration, Supplier<RleDecoder> decoderFactory, SimilarityProc.SimilarityComputer<T> computer) {
        this.terminationFlag = terminationFlag;
        this.configuration = configuration;
        this.decoderFactory = decoderFactory;
        this.computer = computer;
    }

    public Stream<SimilarityResult> stream(T[] inputs, int[] sourceIndexIds, int[] targetIndexIds, double cutoff, int topK) {
        int concurrency = configuration.getConcurrency();

        int length = inputs.length;
        if (concurrency == 1) {
            if (topK != 0) {
                return similarityStreamTopK(inputs, sourceIndexIds, targetIndexIds, length, cutoff, topK, computer, decoderFactory);
            } else {
                return similarityStream(inputs, sourceIndexIds, targetIndexIds, length, cutoff, computer, decoderFactory);
            }
        } else {
            if (topK != 0) {
                return similarityParallelStreamTopK(inputs, sourceIndexIds, targetIndexIds, length, terminationFlag, concurrency, cutoff, topK, computer, decoderFactory);
            } else {
                return similarityParallelStream(inputs, sourceIndexIds, targetIndexIds, length, terminationFlag, concurrency, cutoff, computer, decoderFactory);
            }
        }
    }

    private Stream<SimilarityResult> similarityStream(T[] inputs, int[] sourceIndexIds, int[] targetIndexIds, int length, double cutoff, SimilarityProc.SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {
        RleDecoder decoder = decoderFactory.get();

        IntStream sourceRange = sourceIndexIds.length > 0 ? Arrays.stream(sourceIndexIds) : IntStream.range(0, length);
        Function<Integer, IntStream> targetRange = (sourceId) -> targetIndexIds.length > 0 ? Arrays.stream(targetIndexIds) : IntStream.range(sourceId + 1, length);

        return sourceRange.boxed().flatMap(sourceId -> targetRange.apply(sourceId)
                .mapToObj(targetId -> sourceId == targetId ? null : computer.similarity(decoder, inputs[sourceId], inputs[targetId], cutoff))
                .filter(Objects::nonNull));
    }

    private Stream<SimilarityResult> similarityStream(T[] inputs, int length, double cutoff, SimilarityProc.SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {
        RleDecoder decoder = decoderFactory.get();
        return IntStream.range(0, length)
                .boxed().flatMap(sourceId -> IntStream.range(sourceId + 1, length)
                        .mapToObj(targetId -> computer.similarity(decoder, inputs[sourceId], inputs[targetId], cutoff)).filter(Objects::nonNull));
    }


    private Stream<SimilarityResult> similarityStreamTopK(T[] inputs, int[] sourceIndexIds, int[] targetIndexIds, int length, double cutoff, int topK, SimilarityProc.SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {
        int sourceIdCount = sourceIndexIds.length > 0 ? sourceIndexIds.length : length;
        TopKConsumer<SimilarityResult>[] topKHolder = initializeTopKConsumers(sourceIdCount, topK);
        RleDecoder decoder = decoderFactory.get();

        IntStream sourceRange = sourceIndexIds.length > 0 ? Arrays.stream(sourceIndexIds) : IntStream.range(0, length);
        Function<Integer, IntStream> targetRange = (sourceId) -> targetIndexIds.length > 0 ? Arrays.stream(targetIndexIds) : IntStream.range(sourceId + 1, length);

        SimilarityConsumer consumer = assignSimilarityPairs(topKHolder);
        sourceRange.forEach(sourceId -> computeSimilarityForSourceIndex(sourceId, inputs, cutoff, consumer, computer, decoder, targetRange));

        return Arrays.stream(topKHolder).flatMap(TopKConsumer::stream);
    }


    private void computeSimilarityForSourceIndex(int sourceId, T[] inputs, double cutoff, SimilarityConsumer consumer, SimilarityProc.SimilarityComputer<T> computer, RleDecoder decoder, Function<Integer, IntStream> targetRange) {
        targetRange.apply(sourceId).forEach(targetId -> {
            if(sourceId != targetId) {
                SimilarityResult similarity = computer.similarity(decoder, inputs[sourceId], inputs[targetId], cutoff);

                if (similarity != null) {
                    consumer.accept(sourceId, targetId, similarity);
                }
            }
        });
    }

    private void computeSimilarityForSourceIndex(int sourceId, T[] inputs, int length, double cutoff, SimilarityConsumer consumer, SimilarityProc.SimilarityComputer<T> computer, RleDecoder decoder) {
        for (int targetId = sourceId + 1; targetId < length; targetId++) {
            SimilarityResult similarity = computer.similarity(decoder, inputs[sourceId], inputs[targetId], cutoff);
            if (similarity != null) {
                consumer.accept(sourceId, targetId, similarity);
            }
        }
    }


    public static SimilarityConsumer assignSimilarityPairs(TopKConsumer<SimilarityResult>[] topKConsumers) {
        return (s, t, result) -> {
            topKConsumers[result.reversed ? t : s].accept(result);

            if (result.bidirectional) {
                SimilarityResult reverse = result.reverse();
                topKConsumers[reverse.reversed ? t : s].accept(reverse);
            }
        };
    }

    private TopKConsumer<SimilarityResult>[] initializeTopKConsumers(int length, int topK) {
        Comparator<SimilarityResult> comparator = topK > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        topK = Math.abs(topK);

        TopKConsumer<SimilarityResult>[] results = new TopKConsumer[length];
        for (int i = 0; i < results.length; i++) results[i] = new TopKConsumer<>(topK, comparator);
        return results;
    }

    private  Stream<SimilarityResult> similarityParallelStream(T[] inputs, int[] sourceIndexIds, int[] targetIndexIds, int length, TerminationFlag terminationFlag, int concurrency, double cutoff, SimilarityProc.SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {

        Supplier<IntStream> sourceRange = () ->  sourceIndexIds.length > 0 ?
                Arrays.stream(sourceIndexIds) : IntStream.range(0, length);

        Function<Integer, IntStream> targetRange = (sourceId) -> targetIndexIds.length > 0 ?
                Arrays.stream(targetIndexIds) : IntStream.range(sourceId + 1, length);

        int sourceIdsLength = sourceIndexIds.length > 0 ? sourceIndexIds.length : length;

        int timeout = 100;
        int queueSize = 1000;

        int batchSize = ParallelUtil.adjustBatchSize(sourceIdsLength, concurrency, 1);
        int taskCount = (sourceIdsLength / batchSize) + (sourceIdsLength % batchSize > 0 ? 1 : 0);
        Collection<Runnable> tasks = new ArrayList<>(taskCount);

        ArrayBlockingQueue<SimilarityResult> queue = new ArrayBlockingQueue<>(queueSize);

        int multiplier = batchSize < length ? batchSize : 1;
        for (int taskId = 0; taskId < taskCount; taskId++) {
            int taskOffset = taskId;
            tasks.add(() -> {
                RleDecoder decoder = decoderFactory.get();
                sourceRange.get().skip(taskOffset * multiplier).limit(batchSize).forEach(sourceId ->
                        computeSimilarityForSourceIndex(sourceId, inputs, cutoff, (s, t, result) -> put(queue, result), computer, decoder, targetRange));

            });
        }

        new Thread(() -> {
            try {
                ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);
            } finally {
                put(queue, SimilarityResult.TOMB);
            }
        }).start();

        QueueBasedSpliterator<SimilarityResult> spliterator = new QueueBasedSpliterator<>(queue, SimilarityResult.TOMB, terminationFlag, timeout);
        return StreamSupport.stream(spliterator, false);
    }


    private  void put(BlockingQueue<SimilarityResult> queue, SimilarityResult items) {
        try {
            queue.put(items);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private <T> Stream<SimilarityResult> similarityParallelStreamTopK(T[] inputs, int length, TerminationFlag terminationFlag, int concurrency, double cutoff, int topK, SimilarityProc.SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {
        int batchSize = ParallelUtil.adjustBatchSize(length, concurrency, 1);
        int taskCount = (length / batchSize) + (length % batchSize > 0 ? 1 : 0);
        Collection<TopKTask> tasks = new ArrayList<>(taskCount);

        int multiplier = batchSize < length ? batchSize : 1;
        for (int taskId = 0; taskId < taskCount; taskId++) {
            tasks.add(new TopKTask(batchSize, taskId, multiplier, length, inputs, cutoff, topK, computer, decoderFactory.get()));
        }
        ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);

        TopKConsumer<SimilarityResult>[] topKConsumers = initializeTopKConsumers(length, topK);
        for (Runnable task : tasks) ((TopKTask) task).mergeInto(topKConsumers);
        return Arrays.stream(topKConsumers).flatMap(TopKConsumer::stream);
    }

    private <T> Stream<SimilarityResult> similarityParallelStreamTopK(T[] inputs, int[] sourceIndexIds, int[] targetIndexIds, int length, TerminationFlag terminationFlag, int concurrency, double cutoff, int topK, SimilarityProc.SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {
        Supplier<IntStream> sourceRange = () ->  sourceIndexIds.length > 0 ?
                Arrays.stream(sourceIndexIds) : IntStream.range(0, length);

        Function<Integer, IntStream> targetRange = (sourceId) -> targetIndexIds.length > 0 ?
                Arrays.stream(targetIndexIds) : IntStream.range(sourceId + 1, length);

        int sourceIdsLength = sourceIndexIds.length > 0 ? sourceIndexIds.length : length;

        int batchSize = ParallelUtil.adjustBatchSize(sourceIdsLength, concurrency, 1);
        int taskCount = (sourceIdsLength / batchSize) + (sourceIdsLength % batchSize > 0 ? 1 : 0);
        Collection<NewTopKTask> tasks = new ArrayList<>(taskCount);

        int multiplier = batchSize < length ? batchSize : 1;
        for (int taskId = 0; taskId < taskCount; taskId++) {
            tasks.add(new NewTopKTask(batchSize, taskId, multiplier, length, inputs, cutoff, topK, computer, decoderFactory.get(), sourceRange, targetRange));
        }
        ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);

        TopKConsumer<SimilarityResult>[] topKConsumers = initializeTopKConsumers(sourceIdsLength, topK);
        for (Runnable task : tasks) ((NewTopKTask) task).mergeInto(topKConsumers);
        return Arrays.stream(topKConsumers).flatMap(TopKConsumer::stream);
    }
}
