package com.self.learning.mr.flowsumarea;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

public class FlowAreaPartitioner<K, V> extends Partitioner<K, V> {

    private static Map<String, Integer> areaMap = new HashMap<>();

    static {
        areaMap.put("135", 0);
        areaMap.put("136", 1);
        areaMap.put("137", 2);
        areaMap.put("138", 3);
        areaMap.put("139", 4);
    }

    @Override
    public int getPartition(K key, V value, int numPartitions) {
        return areaMap.get(key.toString().substring(0, 3)) == null ? 5 : areaMap.get(key.toString().substring(0, 3));
    }

}
