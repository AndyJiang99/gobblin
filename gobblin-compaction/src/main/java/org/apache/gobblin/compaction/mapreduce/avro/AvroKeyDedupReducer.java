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

package org.apache.gobblin.compaction.mapreduce.avro;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.gobblin.compaction.mapreduce.RecordKeyDedupReducerBase;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;


/**
 * Reducer class for compaction MR job for Avro data.
 *
 * If there are multiple values of the same key, it keeps the last value read.
 *
 * @author Ziyang Liu
 */
@Slf4j
public class AvroKeyDedupReducer extends RecordKeyDedupReducerBase<AvroKey<GenericRecord>, AvroValue<GenericRecord>,
    AvroKey<GenericRecord>, NullWritable> {

  public static final String DELTA_SCHEMA_PROVIDER =
      "org.apache.gobblin.compaction." + AvroKeyDedupReducer.class.getSimpleName() + ".deltaFieldsProvider";

  @Override
  protected void initReusableObject() {
    outKey = new AvroKey<>();
    outValue = NullWritable.get();
  }

  @Override
  protected void setOutKey(AvroValue<GenericRecord> valueToRetain) {
    outKey.datum(valueToRetain.datum());
  }

  @Override
  protected void setOutValue(AvroValue<GenericRecord> valueToRetain) {
    // do nothing since initReusableObject has assigned value for outValue.
  }

  @Override
  protected void initDeltaComparator(Configuration conf) {
    deltaComparatorOptional = Optional.absent();
    String deltaSchemaProviderClassName = conf.get(DELTA_SCHEMA_PROVIDER);
    if (deltaSchemaProviderClassName != null) {
      deltaComparatorOptional = Optional.of(new AvroValueDeltaSchemaComparator(
          GobblinConstructorUtils.invokeConstructor(AvroDeltaFieldNameProvider.class, deltaSchemaProviderClassName,
              conf)));
    }
  }

  @Override
  protected void reduce(AvroKey<GenericRecord> key, Iterable<AvroValue<GenericRecord>> values, Context context)
      throws IOException, InterruptedException {
    int numVals = 0;
    boolean firstSeen = false;

    AvroValue<GenericRecord> valueToRetain = null;
    Map<Integer, List<GenericRecord>> allRecords = new HashMap<>();
    List<GenericRecord> temp = new ArrayList<>();

    // Preserve only one values among all duplicates.
    for (AvroValue<GenericRecord> value : values) {
      temp.clear();
      if (valueToRetain == null) {
        valueToRetain = value;
      } else if (deltaComparatorOptional.isPresent()) {
        valueToRetain = deltaComparatorOptional.get().compare(valueToRetain, value) >= 0 ? valueToRetain : value;
      }
      GenericRecord record = value.datum();
      try {
        if (!firstSeen) {
          log.info(record.getSchema().toString());
          log.info(record.get("ETL_SCN").toString());
          Integer ETL_SCN = (Integer) record.get("ETL_SCN");
          if (!allRecords.containsKey(ETL_SCN)) {
            temp.add(record);
          }
          allRecords.put(ETL_SCN, temp);
          firstSeen = true;
        }
      } catch (Exception e) {
        log.info(e.getMessage());
      }
      numVals++;
    }

    writeRetainedValue(valueToRetain, context);
    updateCounters(numVals, context);
    printSCN(allRecords);
  }

  protected void printSCN(Map<Integer, List<GenericRecord>> allRecords) {
    for (Map.Entry<Integer, List<GenericRecord>> records : allRecords.entrySet()) {
      if (records.getValue().size() >= 1) {
        log.info("For ETL_SCN of: " + records.getKey() + ", there exists " + records.getValue().size() + " duplicates.");
        for (GenericRecord record : records.getValue()) {
          log.info(record.toString());
        }
      }
    }
  }

  @VisibleForTesting
  protected static class AvroValueDeltaSchemaComparator implements Comparator<AvroValue<GenericRecord>> {
    private final AvroDeltaFieldNameProvider deltaSchemaProvider;

    public AvroValueDeltaSchemaComparator(AvroDeltaFieldNameProvider provider) {
      this.deltaSchemaProvider = provider;
    }

    @Override
    public int compare(AvroValue<GenericRecord> o1, AvroValue<GenericRecord> o2) {
      GenericRecord record1 = o1.datum();
      GenericRecord record2 = o2.datum();
      for (String deltaFieldName : this.deltaSchemaProvider.getDeltaFieldNames(record1)) {
        if (record1.get(deltaFieldName).equals(record2.get(deltaFieldName))) {
          continue;
        }
        return ((Comparable) record1.get(deltaFieldName)).compareTo(record2.get(deltaFieldName));
      }

      return 0;
    }
  }
}
