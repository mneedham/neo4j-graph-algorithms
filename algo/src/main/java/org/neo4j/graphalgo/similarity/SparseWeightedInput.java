package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.core.utils.Intersections;

class SparseWeightedInput implements WeightedInput {
    private final int initialSize;
    private long id;
    double[] weights;
    private int count;

    public SparseWeightedInput(long id, double[] weights, int initialSize, int nonSkipSize) {
        this.initialSize = initialSize;
        this.id = id;
        this.weights = weights;
        this.count = nonSkipSize;
    }

    public int compareTo(WeightedInput o) {
        return Long.compare(id(), o.id());
    }

    public SimilarityResult sumSquareDeltaSkip(double similarityCutoff, WeightedInput other, double skipValue) {
        int len = Math.min(weights().length, other.weights().length);
        double sumSquareDelta = Intersections.sumSquareDeltaSkip(weights(), other.weights(), len, skipValue);
        long intersection = 0;

        if (similarityCutoff >= 0d && sumSquareDelta > similarityCutoff) return null;
        return new SimilarityResult(id(), other.id(), count(), other.count(), intersection, sumSquareDelta);
    }

    public SimilarityResult sumSquareDelta(double similarityCutoff, WeightedInput other) {
        int len = Math.min(weights().length, other.weights().length);
        double sumSquareDelta = Intersections.sumSquareDelta(weights(), other.weights(), len);
        long intersection = 0;

        if (similarityCutoff >= 0d && sumSquareDelta > similarityCutoff) return null;
        return new SimilarityResult(id(), other.id(), count(), other.count(), intersection, sumSquareDelta);
    }

    public SimilarityResult cosineSquaresSkip(RleDecoder decoder, double similarityCutoff, WeightedInput other, double skipValue) {
        decoder.reset(this.weights(), other.weights());

        double[] thisWeights = decoder.item1();
        double[] otherWeights = decoder.item2();

        int len = Math.min(thisWeights.length, otherWeights.length);
        double cosineSquares = Intersections.cosineSquareSkip(thisWeights, otherWeights, len, skipValue);
        long intersection = 0;

        if (similarityCutoff >= 0d && (cosineSquares == 0 || cosineSquares < similarityCutoff)) return null;
        return new SimilarityResult(id(), other.id(), count(), other.count(), intersection, cosineSquares);
    }

    public SimilarityResult cosineSquares(RleDecoder decoder, double similarityCutoff, WeightedInput other) {
        decoder.reset(this.weights(), other.weights());

        double[] thisWeights = decoder.item1();
        double[] otherWeights = decoder.item2();

        int len = Math.min(thisWeights.length, otherWeights.length);
        double cosineSquares = Intersections.cosineSquare(thisWeights, otherWeights, len);
        long intersection = 0;

        if (similarityCutoff >= 0d && (cosineSquares == 0 || cosineSquares < similarityCutoff)) return null;
        return new SimilarityResult(id(), other.id(), count(), other.count(), intersection, cosineSquares);
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public double[] weights() {
        return weights;
    }

    @Override
    public int count() {
        return count;
    }

    @Override
    public int initialSize() {
        return initialSize;
    }
}
