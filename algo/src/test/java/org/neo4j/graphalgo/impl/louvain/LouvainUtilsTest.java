package org.neo4j.graphalgo.impl.louvain;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class LouvainUtilsTest {
    @Test
    public void differentNumbers() {
        int[] communities = {10, 3, 4, 7, 6, 7, 10};
        int[] newCommunities = LouvainUtils.squashCommunities(communities);
        assertArrayEquals(new int[]{0, 1, 2, 3, 4, 3, 0}, newCommunities);
    }

    @Test
    public void allTheSame() {
        int[] communities = {10, 10, 10, 10};
        int[] newCommunities = LouvainUtils.squashCommunities(communities);
        assertArrayEquals(new int[]{0, 0, 0, 0}, newCommunities);
    }

    @Test
    public void allDifferent() {
        int[] communities = {1, 2, 3, 4, 7, 5};
        int[] newCommunities = LouvainUtils.squashCommunities(communities);
        assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5}, newCommunities);
    }
}