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
import java.util.HashMap;
import java.util.Map;
import java.math.BigInteger;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

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


/**
 * Check record duplicates in reducer-side.
 */
public class OrcKeyDedupReducer extends RecordKeyDedupReducerBase<OrcKey, OrcValue, NullWritable, OrcValue> {

  protected EventSubmitter eventSubmitter;
  protected MetricContext metricContext;
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
    Map<Integer, TreeMap<BigInteger, Map<Integer, BigInteger>>> recordFirstView = new HashMap<>();
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
       * Map structure: {hashcode, {logAppendTime, {offset, partition}}}
       */

      TreeMap<BigInteger, Map<Integer, BigInteger>> temp = new TreeMap<>();
      Map<Integer, BigInteger> kafkaInfo = new HashMap<>();
      BigInteger timestamp = BigInteger.valueOf(Long.parseLong(String.valueOf(((OrcStruct)((OrcStruct)value.value).getFieldValue("_kafkaMetadata")).getFieldValue("timestamp"))));
      int partition = Integer.parseInt(String.valueOf(((OrcStruct)((OrcStruct)value.value).getFieldValue("_kafkaMetadata")).getFieldValue("partition")));
      BigInteger offset = BigInteger.valueOf(Long.parseLong(String.valueOf(((OrcStruct)((OrcStruct)value.value).getFieldValue("_kafkaMetadata")).getFieldValue("offset"))));

      if (recordFirstView.containsKey(valueHash)){
        temp = recordFirstView.get(valueHash);
      }
      if (temp.containsKey(timestamp)){
        kafkaInfo.get(timestamp);
      }

      kafkaInfo.put(partition, offset);
      temp.put(timestamp, kafkaInfo);
      recordFirstView.put(valueHash, temp);
    }
    // Emitting the duplicates in the TreeMap
    for (Map.Entry<Integer, TreeMap<BigInteger, Map<Integer, BigInteger>>> hashcode: recordFirstView.entrySet()){
      BigInteger initialTime = BigInteger.valueOf(-1);
      int initialPartition = -1;
      BigInteger initialOffset = BigInteger.valueOf(-1);
      GobblinEventBuilder gobblinTrackingEvent = new GobblinEventBuilder("Gobblin duplicate events - andjiang");

      for (Map.Entry<BigInteger, Map<Integer, BigInteger>> appendTime: hashcode.getValue().entrySet()){
        Set<Map.Entry<Integer, BigInteger>> kafkaInformationSet = appendTime.getValue().entrySet();
        Map.Entry<Integer, BigInteger> kafkaInformation = kafkaInformationSet.iterator().next();
        // Set the values for the first element by hashcode
        if (initialTime.equals(BigInteger.valueOf(-1)) && initialPartition == -1){
          initialTime = appendTime.getKey();
          initialPartition = kafkaInformation.getKey();
          initialOffset = kafkaInformation.getValue();
        }
        else{
          BigInteger newTime = appendTime.getKey();
          BigInteger timeDiff = newTime.subtract(initialTime);

          if(kafkaInformation.getKey() == initialPartition){
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
          gobblinTrackingEvent.addMetadata("partitionCurrentRecord", String.valueOf(kafkaInformation.getKey()));
          gobblinTrackingEvent.addMetadata("offsetFirstRecord", String.valueOf(initialOffset));
          gobblinTrackingEvent.addMetadata("offsetCurrentRecord", String.valueOf(kafkaInformation.getValue()));
          eventSubmitter.submit(gobblinTrackingEvent);
        }
      }
    }

    /* At this point, keyset of valuesToRetain should contains all different OrcValue. */
    for (Map.Entry<Integer, Integer> entry : valuesToRetain.entrySet()) {
      updateCounters(entry.getValue(), context);
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
