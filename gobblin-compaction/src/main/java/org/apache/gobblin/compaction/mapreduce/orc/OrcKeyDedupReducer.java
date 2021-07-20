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

package org.apache.gobblin.compaction.mapreduce.orc;

import com.typesafe.config.Config;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.math.BigInteger;
import java.util.Properties;
import java.util.TreeMap;
import java.util.ArrayList;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.compaction.mapreduce.RecordKeyDedupReducerBase;
import org.apache.gobblin.configuration.DynamicConfigGenerator;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MultiReporterException;
import org.apache.gobblin.runtime.DynamicConfigGeneratorFactory;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.JobConfigurationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import java.time.Instant;

@Slf4j
/**
 * Check record duplicates in reducer-side.
 */
public class OrcKeyDedupReducer extends RecordKeyDedupReducerBase<OrcKey, OrcValue, NullWritable, OrcValue> {

  protected EventSubmitter eventSubmitter;
  protected MetricContext metricContext;
  class KafkaEvent{
    int partition;
    BigInteger offset;
  }
  @VisibleForTesting
  public static final String ORC_DELTA_SCHEMA_PROVIDER =
      "org.apache.gobblin.compaction." + OrcKeyDedupReducer.class.getSimpleName() + ".deltaFieldsProvider";
  public static final String USING_WHOLE_RECORD_FOR_COMPARE = "usingWholeRecordForCompareInReducer";

  @Override
  protected void setup(Context context){
    super.setup(context);
    Properties properties = new Properties();

    Configuration configuration = context.getConfiguration();
    JobConfigurationUtils.putConfigurationIntoProperties(configuration, properties);
    Config config = ConfigUtils.propertiesToConfig(properties);
    DynamicConfigGenerator dynamicConfigGenerator = DynamicConfigGeneratorFactory.createDynamicConfigGenerator(config);
    config = dynamicConfigGenerator.generateDynamicConfig(config).withFallback(config);
    State state = ConfigUtils.configToState(config);

    GobblinMetrics gobblinMetrics = GobblinMetrics.get(context.getTaskAttemptID().toString());
    try{
      gobblinMetrics.startMetricReportingWithFileSuffix(state, context.getTaskAttemptID().toString());
    } catch (MultiReporterException e) {
      e.printStackTrace();
    }
    this.metricContext = gobblinMetrics.getMetricContext().childBuilder("reducer").build();
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, "gobblin.DuplicateEvents").build();
  }

  @Override
  protected void setOutValue(OrcValue valueToRetain) {
    // Better to copy instead reassigning reference.
    outValue.value = valueToRetain.value;
  }

  @Override
  protected void setOutKey(OrcValue valueToRetain) {
    // do nothing since initReusableObject has assigned value for outKey.
  }

  @Override
  protected void reduce(OrcKey key, Iterable<OrcValue> values, Context context)
      throws IOException, InterruptedException {
    /* Map from hash of value(Typed in OrcStruct) object to its times of duplication*/
    Map<Integer, Integer> valuesToRetain = new HashMap<>();
    // New map of values of first record
    Map<Integer, TreeMap<BigInteger, ArrayList<KafkaEvent>>> recordFirstView = new HashMap<>();
    int valueHash = 0;
    String topicName = "";

    for (OrcValue value : values) {
      String originalSchema = ((OrcStruct)value.value).getSchema().toString();
      String noMetadataSchema = originalSchema.replace(",_kafkaMetadata:struct<topic:string,partition:int,offset:bigint,timestamp:bigint,timestampType:struct<noTimestamp:boolean,createTime:boolean,logAppendTime:boolean>,cluster:string,fabric:string>","");
      TypeDescription newSchema = TypeDescription.fromString(noMetadataSchema);
      OrcStruct newRecord = new OrcStruct(newSchema);
      OrcUtils.upConvertOrcStruct((OrcStruct) value.value, newRecord, newSchema);
      valueHash = newRecord.hashCode();
      if (topicName.equals("")){
        topicName = String.valueOf(((OrcStruct)((OrcStruct)value.value).getFieldValue("_kafkaMetadata")).getFieldValue("topic"));
      }

      if (valuesToRetain.containsKey(valueHash)) {
        valuesToRetain.put(valueHash, valuesToRetain.get(valueHash) + 1);
      } else {
        valuesToRetain.put(valueHash, 1);
        writeRetainedValue(value, context);
      }

      /**
       * Add hashcode, logAppendTime, offset, and partition to our map, both duplicates and non-duplicates
       * Map structure: {hashcode, {logAppendTime, [{partition, offset}]}
       */

      TreeMap<BigInteger, ArrayList<KafkaEvent>> temp = new TreeMap<>();
      ArrayList<KafkaEvent> kafkaPartitionOffset = new ArrayList<KafkaEvent>();

      BigInteger timestamp = BigInteger.valueOf(Long.parseLong(String.valueOf(((OrcStruct)((OrcStruct)value.value).getFieldValue("_kafkaMetadata")).getFieldValue("timestamp"))));
      int partition = Integer.parseInt(String.valueOf(((OrcStruct)((OrcStruct)value.value).getFieldValue("_kafkaMetadata")).getFieldValue("partition")));
      BigInteger offset = BigInteger.valueOf(Long.parseLong(String.valueOf(((OrcStruct)((OrcStruct)value.value).getFieldValue("_kafkaMetadata")).getFieldValue("offset"))));

      if (recordFirstView.containsKey(valueHash)){
        temp = recordFirstView.get(valueHash);
      }
      // If the logAppendTime already exists
      if (temp.containsKey(timestamp)){
        kafkaPartitionOffset = temp.get(timestamp);
      }
      KafkaEvent newEvent = new KafkaEvent();
      newEvent.partition = partition;
      newEvent.offset = offset;

      kafkaPartitionOffset.add(newEvent);
      temp.put(timestamp, kafkaPartitionOffset);
      recordFirstView.put(valueHash, temp);
    }
    presetEnums(context);
    emitEvents(topicName, recordFirstView, context);

    /* At this point, keyset of valuesToRetain should contains all different OrcValue. */
    for (Map.Entry<Integer, Integer> entry : valuesToRetain.entrySet()) {
      updateCounters(entry.getValue(), context);
    }
  }

  private void emitEvents(String topicName, Map<Integer, TreeMap<BigInteger, ArrayList<KafkaEvent>>> recordFirstView, Context context){
    for (Map.Entry<Integer, TreeMap<BigInteger, ArrayList<KafkaEvent>>> hashcode: recordFirstView.entrySet()){
      BigInteger initialTime = BigInteger.valueOf(-1);
      int initialPartition = -1;
      BigInteger initialOffset = BigInteger.valueOf(-1);

      int currentPartition = -1;
      BigInteger currentOffset = BigInteger.valueOf(-1);
      GobblinEventBuilder gobblinTrackingEvent = new GobblinEventBuilder("Gobblin duplicate events - andjiang");

      // Go through all the logAppendTimes for the same hashcode
      for (Map.Entry<BigInteger, ArrayList<KafkaEvent>> appendTime: hashcode.getValue().entrySet()){
        Comparator<KafkaEvent> comparator = Comparator.comparing(event -> event.partition);
        comparator = comparator.thenComparing(event -> event.offset);
        Stream<KafkaEvent> sorted = appendTime.getValue().stream().sorted(comparator);
        ArrayList<KafkaEvent> sortedKafkaEvents = new ArrayList<>(sorted.collect(Collectors.toList()));

        for (int i = 0; i < sortedKafkaEvents.size(); i++){
          if (initialTime.equals(BigInteger.valueOf(-1)) && initialPartition == -1 && initialOffset.equals(BigInteger.valueOf(-1))){
            initialTime = appendTime.getKey();
            initialPartition = currentPartition = hashcode.getValue().firstEntry().getValue().get(0).partition;
            initialOffset = currentOffset = hashcode.getValue().firstEntry().getValue().get(0).offset;
          }

          else{
            if (currentPartition == sortedKafkaEvents.get(i).partition
                && currentOffset.equals(sortedKafkaEvents.get(i).offset)){
              log.info("Topic: " + topicName + ", Record Time: " + appendTime.getKey() + ", Partition: " + sortedKafkaEvents.get(i).partition + ", Offset: " + sortedKafkaEvents.get(i).offset);
              updateExactDuplicateCounter(1, context);
            }
            else {
              BigInteger newTime = appendTime.getKey();
              BigInteger timeDiff = newTime.subtract(initialTime);
              currentPartition = sortedKafkaEvents.get(i).partition;
              currentOffset = sortedKafkaEvents.get(i).offset;

              BigInteger timeDiffMinutes = timeDiff.divide(BigInteger.valueOf(1000)).divide(BigInteger.valueOf(60));

              updateTimeRangeCounter(timeDiffMinutes.intValue() / 5, context);
              setLargestRange(timeDiff.longValue(), context);

              if (topicName.equals("InvitationScoreEvent")
                  || topicName.equals("MooJobPostingsRankingEvent")
                  || topicName.equals("ZephyrConversationsImpressionEvent")
                  || topicName.equals("ZephyrMessageReceivedEvent")
                  || topicName.equals("ZephyrConversationDetailImpressionEvent")
                  || topicName.equals("fx-lifecycle-event")
                  || topicName.equals("feed-indexing-tensor-features")
                  || topicName.equals("MemberCustomerMap")
                  || topicName.equals("frame-monitor-online")
                  || topicName.equals("SecurityHeaderErrorEvent")
                  || topicName.equals("ContentFilteringEvent")
                  || (topicName.equals("LixTreatmentsEvent") && timeDiffMinutes.compareTo(BigInteger.valueOf(15)) < 0)){
                break;
              }

              if (sortedKafkaEvents.get(i).partition == initialPartition){
                gobblinTrackingEvent.addMetadata("partitionSimilarity", String.valueOf(true));
              }
              else{
                gobblinTrackingEvent.addMetadata("partitionSimilarity", String.valueOf(false));
              }

              gobblinTrackingEvent.addMetadata("topic", topicName);
              gobblinTrackingEvent.addMetadata("timeFirstRecord", String.valueOf(initialTime));
              gobblinTrackingEvent.addMetadata("timeCurrentRecord", String.valueOf(newTime));
              gobblinTrackingEvent.addMetadata("timeDiff", String.valueOf(timeDiff));
              gobblinTrackingEvent.addMetadata("partitionFirstRecord", String.valueOf(initialPartition));
              gobblinTrackingEvent.addMetadata("partitionCurrentRecord", String.valueOf(currentPartition));
              gobblinTrackingEvent.addMetadata("offsetFirstRecord", String.valueOf(initialOffset));
              gobblinTrackingEvent.addMetadata("offsetCurrentRecord", String.valueOf(currentOffset));
              gobblinTrackingEvent.addMetadata("eventUnixTime", String.valueOf(Instant.now().toEpochMilli()));
              eventSubmitter.submit(gobblinTrackingEvent);
              updateGTECounters(1, context);
            }
          }
        }
      }
    }
  }

  @Override
  protected void initDeltaComparator(Configuration conf) {
    deltaComparatorOptional = Optional.absent();
  }

  @Override
  protected void initReusableObject() {
    outKey = NullWritable.get();
    outValue = new OrcValue();
  }
}

