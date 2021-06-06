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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.math.BigInteger;
import java.math.*;

import java.util.TreeMap;
import org.apache.gobblin.compaction.mapreduce.RecordKeyDedupReducerBase;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
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

import lombok.extern.slf4j.Slf4j;

/**
 * Check record duplicates in reducer-side.
 */
@Slf4j
public class OrcKeyDedupReducer extends RecordKeyDedupReducerBase<OrcKey, OrcValue, NullWritable, OrcValue> {

  protected EventSubmitter eventSubmitter;
  protected MetricContext metricContext;
  @VisibleForTesting
  public static final String ORC_DELTA_SCHEMA_PROVIDER =
      "org.apache.gobblin.compaction." + OrcKeyDedupReducer.class.getSimpleName() + ".deltaFieldsProvider";
  public static final String USING_WHOLE_RECORD_FOR_COMPARE = "usingWholeRecordForCompareInReducer";

  public OrcKeyDedupReducer(){
    this.metricContext = Instrumented.getMetricContext(new org.apache.gobblin.configuration.State(), this.getClass());
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, "gobblinDupEvents").build();
    log.info("Constructed metricContext and eventSubmitter");
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
    // new map of values of first record
    Map<Integer, TreeMap<BigInteger, Integer>> recordFirstView = new HashMap<Integer, TreeMap<BigInteger, Integer>>();
    int valueHash = 0;
    String topicName = "";

    for (OrcValue value : values) {
      log.info(String.valueOf(value.value));
      String originalSchema = ((OrcStruct)value.value).getSchema().toString();
      String noMetadataSchema = originalSchema.replace(",_kafkaMetadata:struct<topic:string,partition:int,offset:bigint,timestamp:bigint,timestampType:struct<noTimestamp:boolean,createTime:boolean,logAppendTime:boolean>,cluster:string,fabric:string>","");
      TypeDescription newSchema = TypeDescription.fromString(noMetadataSchema);
      OrcStruct newRecord = new OrcStruct(newSchema);
      OrcUtils.upConvertOrcStruct((OrcStruct) value.value, newRecord, newSchema);
      valueHash = newRecord.hashCode();
      log.info("value hash: " + valueHash);
      if (topicName.equals("")){
        topicName = String.valueOf(((OrcStruct)((OrcStruct)value.value).getFieldValue("_kafkaMetadata")).getFieldValue("topic"));
      }

      // Add uuid, logAppendTime originally, and partition to our map, both duplicates and non-duplicates
      TreeMap<BigInteger, Integer> temp = new TreeMap<>();
      BigInteger timestamp = BigInteger.valueOf(Long.parseLong(String.valueOf(((OrcStruct)((OrcStruct)value.value).getFieldValue("_kafkaMetadata")).getFieldValue("timestamp"))));
      int partition = Integer.parseInt(String.valueOf(((OrcStruct)((OrcStruct)value.value).getFieldValue("_kafkaMetadata")).getFieldValue("partition")));
      if (recordFirstView.containsKey(valueHash)){
        log.info("Saw a duplicate");
        temp = recordFirstView.get(valueHash);
        log.info(temp.toString());
      }
      log.info("Adding a new record");
      temp.put(timestamp, partition);
      log.info(temp.toString());
      recordFirstView.put(valueHash, temp);
      log.info("Done adding to map");

      if (valuesToRetain.containsKey(valueHash)) {
        log.info("DELETEDUPLICATES");
        valuesToRetain.put(valueHash, valuesToRetain.get(valueHash) + 1);
      } else {
        valuesToRetain.put(valueHash, 1);
        writeRetainedValue(value, context);
      }
    }

    log.info("Finished reducing. About to emit duplicate events");
    log.info(recordFirstView.toString());
    // Emitting the duplicates in the TreeMap
    for (Map.Entry<Integer, TreeMap<BigInteger, Integer>> hashcode: recordFirstView.entrySet()){
      BigInteger initialTime = BigInteger.valueOf(-1);
      int initialPartition = -1;
      GobblinEventBuilder gobblinTrackingEvent = new GobblinEventBuilder("Gobblin duplicate events - andjiang");
      log.info("Instantiated Gobblin Tracking Event");
      log.info(gobblinTrackingEvent.toString());
      log.info("Length of inner treemap " + hashcode.getValue().size());
      for (Map.Entry<BigInteger, Integer> appendTime: hashcode.getValue().entrySet()){
        // This is to set the values for the first element by hashcode
        log.info("Looping appendTime map");
        if (initialTime.equals(BigInteger.valueOf(-1)) && initialPartition == -1){
          log.info("Setting time and partition for first record");
          initialTime = appendTime.getKey();
          log.info("Initial Time");
          log.info(String.valueOf(initialTime));
          initialPartition = appendTime.getValue();
          log.info("Initial Partition");
          log.info(String.valueOf(initialPartition));
        }
        else{
          log.info("Starting to emit events for duplicates: " + hashcode.getKey());
          log.info(topicName);
          gobblinTrackingEvent.addMetadata("topic", topicName);
          BigInteger newTime = appendTime.getKey();
          BigInteger timeDiff = newTime.subtract(initialTime);
          gobblinTrackingEvent.addMetadata("timeDiff", String.valueOf(timeDiff));
          if(appendTime.getValue() == initialPartition){
            gobblinTrackingEvent.addMetadata("partitionSimilarity", String.valueOf(true));
          }
          else{
            gobblinTrackingEvent.addMetadata("partitionSimilarity", String.valueOf(false));
          }
          log.info("Attempting to submit event");
          log.info("Gobblin Tracking Event");
          log.info(gobblinTrackingEvent.toString());
          eventSubmitter.submit(gobblinTrackingEvent);
          log.info("Finished submitting event");
        }
        log.info("Testing GTE submission");
        gobblinTrackingEvent.addMetadata("testing", String.valueOf(true));
        eventSubmitter.submit(gobblinTrackingEvent);
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
