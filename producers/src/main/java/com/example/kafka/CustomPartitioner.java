package com.example.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    private static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class);
    private static final StickyPartitionCache STICKY_PARTITION_CACHE = new StickyPartitionCache();
    private static double specialPartitionRatio;
    private static String specialKeyName;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (keyBytes == null) {
            return STICKY_PARTITION_CACHE.partition(topic, cluster);
            //throw new InvalidRecordException("key should not be null");
        }

        // 파티션 정보를 가지는 리스트
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);

        // 파티션 수
        int numberOfPartitions = partitionInfos.size();

        // 특정 Key 특별히 할당할 파티션 수
        int numberOfSpecialPartitions = (int) (numberOfPartitions * specialPartitionRatio);

        int partitionIndex;

        if (key.equals(specialKeyName)) {
            // 해싱이 Special Key 1개만 된다면 numberOfSpecialPartitions 가 여러개일 때, 나머지가 항상 같으므로 분산되지 않음
            // 따라서 valueBytes 를 해싱해서 사용하면 numberOfSpecialPartitions 에 맞게 분산될 것
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numberOfSpecialPartitions;

        } else {
            partitionIndex = Utils.toPositive(Utils.murmur2(keyBytes))
                    // 나머지는 전체 파티션에서 특별히 할당되는 파티션은 제외된 곳으로 가야함
                    % (numberOfPartitions - numberOfSpecialPartitions) + numberOfSpecialPartitions;
        }

        logger.info("key:{} is sent to partition:{}", key, partitionIndex);

        return partitionIndex;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        specialPartitionRatio = Double.parseDouble(configs.get("custom.specialKey.partitionRatio").toString());
        specialKeyName = configs.get("custom.specialKey").toString();
    }

}
