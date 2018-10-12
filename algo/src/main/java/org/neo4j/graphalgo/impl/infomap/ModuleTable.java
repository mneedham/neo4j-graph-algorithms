package org.neo4j.graphalgo.impl.infomap;

import org.neo4j.graphalgo.core.utils.ArrayUtil;

/**
 *
 * @author mknblch
 */
public class ModuleTable {

    private int[] source, target;
    private double[] n1, n2, p1, p2, w1, w2, w12, q1, q2, dL;

    public ModuleTable(int initialCapacity) {
        source = new int[initialCapacity];
        target = new int[initialCapacity];
        n1 = new double[initialCapacity];
        n2 = new double[initialCapacity];
        p1 = new double[initialCapacity];
        p2 = new double[initialCapacity];
        w1 = new double[initialCapacity];
        w2 = new double[initialCapacity];
        w12 = new double[initialCapacity];
        q1 = new double[initialCapacity];
        q2 = new double[initialCapacity];
        dL = new double[initialCapacity];
    }

//    public void add( )

    public ModuleTable resize(int length) {

        if (length < source.length) {
            return this;
        }

        int ilength = org.apache.lucene.util.ArrayUtil.oversize(length, 4);
        int dlength = org.apache.lucene.util.ArrayUtil.oversize(length, 8);

        source = resize(source, ilength);
        target = resize(target, ilength);
        n1 = resize(n1, dlength);
        n2 = resize(n2, dlength);
        p1 = resize(p1, dlength);
        p2 = resize(p2, dlength);
        w1 = resize(w1, dlength);
        w2 = resize(w2, dlength);
        w12 = resize(w12, dlength);
        q1 = resize(q1, dlength);
        q2 = resize(q1, dlength);
        dL = resize(dL, dlength);

        return this;
    }

    public int getSource(int id) {
        return source[id];
    }

    public int getTarget(int id) {
        return target[id];
    }

    public double getN1(int id) {
        return n1[id];
    }

    public double getN2(int id) {
        return n2[id];
    }

    public double getP1(int id) {
        return p1[id];
    }

    public double getP2(int id) {
        return p2[id];
    }

    public double getW1(int id) {
        return w1[id];
    }

    public double getW2(int id) {
        return w2[id];
    }

    public double getW12(int id) {
        return w12[id];
    }

    public double getQ1(int id) {
        return q1[id];
    }

    public double getQ2(int id) {
        return q2[id];
    }

    public double getDeltaL(int id) {
        return dL[id];
    }

    private static int[] resize(int[] v, int length) {
        final int[] temp = new int[length];
        System.arraycopy(v, 0, temp, 0, v.length);
        return temp;
    }

    private static double[] resize(double[] v, int length) {
        final double[] temp = new double[length];
        System.arraycopy(v, 0, temp, 0, v.length);
        return temp;
    }
}
