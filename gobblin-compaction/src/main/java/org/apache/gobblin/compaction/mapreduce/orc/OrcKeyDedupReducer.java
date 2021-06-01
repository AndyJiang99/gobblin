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
    Map<Integer, TreeMap<Integer, Integer>> recordFirstView = new HashMap<Integer, TreeMap<Integer, Integer>>();
    int valueHash = 0;
    String topicName = "";

    for (OrcValue value : values) {
      String originalSchema = ((OrcStruct)value.value).getSchema().toString();
      String noMetadataSchema = originalSchema.replace(",_kafkaMetadata:struct<topic:string,partition:int,offset:bigint,timestamp:bigint,timestampType:struct<noTimestamp:boolean,createTime:boolean,logAppendTime:boolean>,cluster:string,fabric:string>","");
      TypeDescription newSchema = TypeDescription.fromString(noMetadataSchema);
      OrcStruct newRecord = new OrcStruct(newSchema);
      OrcUtils.upConvertOrcStruct((OrcStruct) value.value, newRecord, newSchema);
      valueHash = newRecord.hashCode();

      if (topicName == ""){
        topicName = String.valueOf(newRecord.getFieldValue("_kafkaMetadata.topic"));
      }
      if (valuesToRetain.containsKey(valueHash)) {
        valuesToRetain.put(valueHash, valuesToRetain.get(valueHash) + 1);
      } else {
        valuesToRetain.put(valueHash, 1);
        writeRetainedValue(value, context);
      }
      // Add uuid, logAppendTime originally, and partition to our map, both duplicates and non-duplicates
      TreeMap<Integer, Integer> temp = new TreeMap<>();
      temp.put(Integer.parseInt(String.valueOf(newRecord.getFieldValue("_kafkaMetadata.timestamp"))), Integer.parseInt(
          String.valueOf(newRecord.getFieldValue("_kafkaMetadata.partition"))));
      recordFirstView.put(valueHash, temp);
    }
    this.metricContext = Instrumented.getMetricContext(new org.apache.gobblin.configuration.State(), this.getClass());
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, "gobblinDupEvents").build();

    // Emitting the duplicates in the TreeMap
    for (Map.Entry<Integer, TreeMap<Integer, Integer>> hashcode: recordFirstView.entrySet()){
      int initialTime = -1;
      int initialPartition = -1;
      GobblinEventBuilder gobblinTrackingEvent =
          new GobblinEventBuilder("Gobblin duplicate events - andjiang");
      for (Map.Entry<Integer, Integer> appendTime: hashcode.getValue().entrySet()){
        // This is to set the values for the first element by hashcode
        if (initialTime == -1 && initialPartition == -1){
          initialTime = appendTime.getKey();
          initialPartition = appendTime.getValue();
        }
        else{
          gobblinTrackingEvent.addMetadata("topic", topicName);
          gobblinTrackingEvent.addMetadata("timeDiff", String.valueOf(appendTime.getKey() - initialTime));
          if(appendTime.getValue() == initialPartition){
            System.out.print("True");
            gobblinTrackingEvent.addMetadata("partitionSimilarity", String.valueOf(true));
          }
          else{
            gobblinTrackingEvent.addMetadata("partitionSimilarity", String.valueOf(false));
          }
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
