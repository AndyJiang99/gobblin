/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.source.extractor.extract.kafka;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;


/**
 * A Utils class for Kafka.
 */
@Slf4j
public class KafkaUtils {

  private static final String TOPIC_PARTITION_DELIMITER = "-";

  /**
   * Get topic name from a {@link State} object. The {@link State} should contain property
   * {@link KafkaSource#TOPIC_NAME}.
   */
  public static String getTopicName(State state) {
    Preconditions.checkArgument(state.contains(KafkaSource.TOPIC_NAME),
        "Missing configuration property " + KafkaSource.TOPIC_NAME);

    return state.getProp(KafkaSource.TOPIC_NAME);
  }

  /**
   * Get {@link KafkaPartition} from a {@link State} object. The {@link State} should contain properties
   * {@link KafkaSource#TOPIC_NAME}, {@link KafkaSource#PARTITION_ID}, and may optionally contain
   * {@link KafkaSource#LEADER_ID} and {@link KafkaSource#LEADER_HOSTANDPORT}.
   */
  public static KafkaPartition getPartition(State state) {
    Preconditions.checkArgument(state.contains(KafkaSource.TOPIC_NAME),
        "Missing configuration property " + KafkaSource.TOPIC_NAME);
    Preconditions.checkArgument(state.contains(KafkaSource.PARTITION_ID),
        "Missing configuration property " + KafkaSource.PARTITION_ID);

    KafkaPartition.Builder builder = new KafkaPartition.Builder().withTopicName(state.getProp(KafkaSource.TOPIC_NAME))
        .withId(state.getPropAsInt(KafkaSource.PARTITION_ID));
    if (state.contains(KafkaSource.LEADER_ID)) {
      builder = builder.withLeaderId(state.getPropAsInt(KafkaSource.LEADER_ID));
    }
    if (state.contains(KafkaSource.LEADER_HOSTANDPORT)) {
      builder = builder.withLeaderHostAndPort(state.getProp(KafkaSource.LEADER_HOSTANDPORT));
    }
    return builder.build();
  }

  /**
   * Get a list of {@link KafkaPartition}s from a {@link State} object. The given {@link State} should contain property
   * {@link KafkaSource#TOPIC_NAME}. If there are multiple partitions in the {@link State}, all partitions should have
   * the same topic name.
   *
   * It first checks whether the given {@link State} contains "partition.id.i", "leader.id.i" and
   * "leader.hostandport.i", i = 0,1,2,...
   *
   * Otherwise it will call {@link #getPartition(State)}.
   */
  public static List<KafkaPartition> getPartitions(State state) {
    List<KafkaPartition> partitions = Lists.newArrayList();
    String topicName = state.getProp(KafkaSource.TOPIC_NAME);
    for (int i = 0;; i++) {
      if (!state.contains(KafkaUtils.getPartitionPropName(KafkaSource.PARTITION_ID, i))) {
        break;
      }
      KafkaPartition.Builder builder = new KafkaPartition.Builder().withTopicName(topicName)
          .withId(state.getPropAsInt(KafkaUtils.getPartitionPropName(KafkaSource.PARTITION_ID, i)));
      String partitionLeaderIdProperName = KafkaUtils.getPartitionPropName(KafkaSource.LEADER_ID, i);
      String partitionLeaderHostPortProperName = KafkaUtils.getPartitionPropName(KafkaSource.LEADER_HOSTANDPORT, i);
      if (state.contains(partitionLeaderIdProperName)) {
        builder = builder.withLeaderId(state.getPropAsInt(partitionLeaderIdProperName));
      }
      if (state.contains(partitionLeaderHostPortProperName)) {
        builder = builder.withLeaderHostAndPort(state.getProp(partitionLeaderHostPortProperName));
      }
      KafkaPartition partition = builder.build();
      partitions.add(partition);
    }
    if (partitions.isEmpty()) {
      partitions.add(getPartition(state));
    }
    return partitions;
  }

  /**
   * This method returns topicName + '.' + partitionId.
   */
  public static String getPartitionPropName(String topicName, int partitionId) {
    return topicName + "." + partitionId;
  }

  /**
   * Determines whether the given {@link State} contains "[topicname].[partitionid].avg.record.size".
   */
  public static boolean containsPartitionAvgRecordSize(State state, KafkaPartition partition) {
    return state.contains(
        getPartitionPropName(partition.getTopicName(), partition.getId()) + "." + ConfigurationKeys.AVG_RECORD_SIZE);
  }

  /**
   * Get the average record size of a partition, which is stored in property "[topicname].[partitionid].avg.record.size".
   * If state doesn't contain this property, it returns defaultSize.
   */
  public static long getPartitionAvgRecordSize(State state, KafkaPartition partition) {
    return state.getPropAsLong(
        getPartitionPropName(partition.getTopicName(), partition.getId()) + "." + ConfigurationKeys.AVG_RECORD_SIZE);
  }

  /**
   * Set the average record size of a partition, which will be stored in property
   * "[topicname].[partitionid].avg.record.size".
   */
  public static void setPartitionAvgRecordSize(State state, KafkaPartition partition, long size) {
    state.setProp(getPartitionPropName(partition.getTopicName(), partition.getId()) + "." + ConfigurationKeys.AVG_RECORD_SIZE,
        size);
  }

  /**
   * Determines whether the given {@link State} contains "[topicname].[partitionid].avg.record.millis".
   */
  public static boolean containsPartitionAvgRecordMillis(State state, KafkaPartition partition) {
    return state.contains(
        getPartitionPropName(partition.getTopicName(), partition.getId()) + "." + KafkaSource.AVG_RECORD_MILLIS);
  }

  /**
   * Get the average time to pull a record of a partition, which is stored in property
   * "[topicname].[partitionid].avg.record.millis". If state doesn't contain this property, it returns defaultValue.
   */
  public static double getPartitionAvgRecordMillis(State state, KafkaPartition partition) {
    double avgRecordMillis = state.getPropAsDouble(
        getPartitionPropName(partition.getTopicName(), partition.getId()) + "." + KafkaSource.AVG_RECORD_MILLIS);

    // cap to prevent a poorly behaved topic from impacting the bin-packing
    int avgFetchTimeCap = state.getPropAsInt(ConfigurationKeys.KAFKA_SOURCE_AVG_FETCH_TIME_CAP,
            ConfigurationKeys.DEFAULT_KAFKA_SOURCE_AVG_FETCH_TIME_CAP);

    if (avgFetchTimeCap > 0 && avgRecordMillis > avgFetchTimeCap) {
      log.info("Topic {} partition {} has an average fetch time of {}, capping it to {}", partition.getTopicName(),
                partition.getId(), avgRecordMillis, avgFetchTimeCap);
      avgRecordMillis = avgFetchTimeCap;
    }

    return avgRecordMillis;
  }

  /**
   * Set the average time in milliseconds to pull a record of a partition, which will be stored in property
   * "[topicname].[partitionid].avg.record.millis".
   */
  public static void setPartitionAvgRecordMillis(State state, KafkaPartition partition, double millis) {
    state.setProp(
        getPartitionPropName(partition.getTopicName(), partition.getId()) + "." + KafkaSource.AVG_RECORD_MILLIS,
        millis);
  }

  /**
   * Get a property as long from a work unit that may or may not be a multiworkunit.
   * This method is needed because the SingleLevelWorkUnitPacker does not squeeze work units
   * into a multiworkunit, and thus does not append the partitionId to property keys, while
   * the BiLevelWorkUnitPacker does.
   * Return 0 as default if key not found in either form.
   */
  public static long getPropAsLongFromSingleOrMultiWorkUnitState(WorkUnitState workUnitState,
                                                                 String key, int partitionId) {
    return Long.parseLong(workUnitState.contains(key) ? workUnitState.getProp(key)
        : workUnitState.getProp(KafkaUtils.getPartitionPropName(key, partitionId), "0"));
  }

  /**
   * Get topic name from a topic partition
   * @param topicPartition
   */
  public static String getTopicNameFromTopicPartition(String topicPartition) {
    Preconditions.checkArgument(topicPartition.contains(TOPIC_PARTITION_DELIMITER));
    List<String> parts = Splitter.on(TOPIC_PARTITION_DELIMITER).splitToList(topicPartition);
    return Joiner.on(TOPIC_PARTITION_DELIMITER).join(parts.subList(0, parts.size() - 1));
  }

}
