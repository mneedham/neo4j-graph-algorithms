package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.IntIntMap;

public class LouvainUtils {

    public static int[] squashCommunities(int[] communities) {
        int[] newCommunities = new int[communities.length];

        IntIntMap communityMapping = new IntIntHashMap();

        int communityCounter = 0;
        for (int i = 0; i < communities.length; i++) {
            int community = communities[i];
            if (communityMapping.containsKey(community)) {
                newCommunities[i] = communityMapping.get(community);
            } else {
                communityMapping.put(community, communityCounter);
                newCommunities[i] = communityCounter++;
            }
        }
        return newCommunities;
    }
}
