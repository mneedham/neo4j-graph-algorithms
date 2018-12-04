package org.neo4j.graphalgo.similarity;

public interface WeightedInput extends Comparable<WeightedInput> {
    SimilarityResult sumSquareDeltaSkip(double similarityCutoff, WeightedInput other, double skipValue);

    SimilarityResult sumSquareDelta(double similarityCutoff, WeightedInput other);

    SimilarityResult cosineSquaresSkip(double similarityCutoff, WeightedInput other, double skipValue);

    SimilarityResult cosineSquares(double similarityCutoff, WeightedInput other);

    long id();

    double[] weights();

    int count();
}
