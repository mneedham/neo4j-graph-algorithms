package org.neo4j.graphalgo.similarity;

public class RleTransformer {
    public static final int REPEAT_CUTOFF = 3;

    public static double[] decode(double[] rleVector, int initialSize) {
        RleReader reader = new RleReader(initialSize);
        return reader.read();
    }

    public static double[] decode(double[] rleVector, RleReader reader, double[] fullVector) {
        reader.reset(rleVector);

        for (int i = 0; i < fullVector.length; i++) {
            reader.next();
            fullVector[i] = reader.value();
        }
        return fullVector;
    }
}
