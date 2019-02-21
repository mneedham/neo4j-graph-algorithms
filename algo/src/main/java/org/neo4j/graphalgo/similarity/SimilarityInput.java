package org.neo4j.graphalgo.similarity;

import java.util.Arrays;
import java.util.List;

public interface SimilarityInput {
    long getId();

    static int[] indexes(long[] inputIds, List<Long> idsToFind) {
        int[] indexes = new int[idsToFind.size()];

        int indexesFound = 0;
        for (int i = 0; i < idsToFind.size(); i++) {
            int index = Arrays.binarySearch(inputIds, idsToFind.get(i));
            if(index >= 0) {
                indexes[indexesFound] = index;
                indexesFound++;
            }
        }

        return Arrays.copyOfRange(indexes, 0, indexesFound);
    }
    static long[] extractInputIds(SimilarityInput[] inputs) {
        return Arrays.stream(inputs).mapToLong(SimilarityInput::getId).toArray();
    }

}
