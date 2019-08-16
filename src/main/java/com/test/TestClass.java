package com.test;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratedClass;

public class TestClass extends GeneratedClass {

  public Object generate(Object[] references) {
    return new GeneratedIteratorForCodegenStage1(references);
  }

  /*wsc_codegenStageId*/
  final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
    private Object[] references;
    private scala.collection.Iterator[] inputs;
    private boolean agg_initAgg_0;
    private boolean agg_bufIsNull_0;
    private long agg_bufValue_0;
    private boolean agg_bufIsNull_1;
    private long agg_bufValue_1;
    private boolean agg_initAgg_1;
    private boolean agg_bufIsNull_2;
    private long agg_bufValue_2;
    private boolean agg_bufIsNull_3;
    private long agg_bufValue_3;
    private scala.collection.Iterator dynamiclocaltablescan_input_0;
    private boolean agg_agg_isNull_11_0;
    private boolean agg_agg_isNull_18_0;
    private boolean agg_agg_isNull_28_0;
    private boolean agg_agg_isNull_30_0;
    private boolean agg_agg_isNull_36_0;
    private boolean agg_agg_isNull_38_0;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] filter_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[6];

    public GeneratedIteratorForCodegenStage1(Object[] references) {
      this.references = references;
    }

    public void init(int index, scala.collection.Iterator[] inputs) {
      partitionIndex = index;
      this.inputs = inputs;

      dynamiclocaltablescan_input_0 = inputs[0];
      filter_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(3, 32);
      filter_mutableStateArray_0[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 0);
      filter_mutableStateArray_0[2] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 0);
      filter_mutableStateArray_0[3] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 0);
      filter_mutableStateArray_0[4] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 0);
      filter_mutableStateArray_0[5] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 0);

    }

    private void agg_doAggregateWithoutKey_0() throws java.io.IOException {
// initialize aggregation buffer
      agg_bufIsNull_0 = true;
      agg_bufValue_0 = -1L;
      agg_bufIsNull_1 = true;
      agg_bufValue_1 = -1L;

      while (!agg_initAgg_1) {
        agg_initAgg_1 = true;
        long agg_beforeAgg_0 = System.nanoTime();
        agg_doAggregateWithoutKey_1();
        ((org.apache.spark.sql.execution.metric.SQLMetric) references[3] /* aggTime */).add((System.nanoTime() - agg_beforeAgg_0) / 1000000);

// output the result

        ((org.apache.spark.sql.execution.metric.SQLMetric) references[2] /* numOutputRows */).add(1);
        agg_doConsume_1(agg_bufValue_2, agg_bufIsNull_2, agg_bufValue_3, agg_bufIsNull_3);
      }

    }

    private void agg_doConsume_1(long agg_expr_0_1, boolean agg_exprIsNull_0_0, long agg_expr_1_1, boolean agg_exprIsNull_1_0) throws java.io.IOException {
// do aggregate
// common sub-expressions

// evaluate aggregate function
      agg_agg_isNull_28_0 = true;
      long agg_value_28 = -1L;
      do {
        boolean agg_isNull_29 = true;
        long agg_value_29 = -1L;
        agg_agg_isNull_30_0 = true;
        long agg_value_30 = -1L;
        do {
          if (!agg_bufIsNull_0) {
            agg_agg_isNull_30_0 = false;
            agg_value_30 = agg_bufValue_0;
            continue;
          }

          boolean agg_isNull_32 = false;
          long agg_value_32 = -1L;
          if (!false) {
            agg_value_32 = (long) 0;
          }
          if (!agg_isNull_32) {
            agg_agg_isNull_30_0 = false;
            agg_value_30 = agg_value_32;
            continue;
          }

        } while (false);

        if (!agg_exprIsNull_0_0) {
          agg_isNull_29 = false; // resultCode could change nullability.

          agg_value_29 = agg_value_30 + agg_expr_0_1;

        }
        if (!agg_isNull_29) {
          agg_agg_isNull_28_0 = false;
          agg_value_28 = agg_value_29;
          continue;
        }

        if (!agg_bufIsNull_0) {
          agg_agg_isNull_28_0 = false;
          agg_value_28 = agg_bufValue_0;
          continue;
        }

      } while (false);
      agg_agg_isNull_36_0 = true;
      long agg_value_36 = -1L;
      do {
        boolean agg_isNull_37 = true;
        long agg_value_37 = -1L;
        agg_agg_isNull_38_0 = true;
        long agg_value_38 = -1L;
        do {
          if (!agg_bufIsNull_1) {
            agg_agg_isNull_38_0 = false;
            agg_value_38 = agg_bufValue_1;
            continue;
          }

          boolean agg_isNull_40 = false;
          long agg_value_40 = -1L;
          if (!false) {
            agg_value_40 = (long) 0;
          }
          if (!agg_isNull_40) {
            agg_agg_isNull_38_0 = false;
            agg_value_38 = agg_value_40;
            continue;
          }

        } while (false);

        if (!agg_exprIsNull_1_0) {
          agg_isNull_37 = false; // resultCode could change nullability.

          agg_value_37 = agg_value_38 + agg_expr_1_1;

        }
        if (!agg_isNull_37) {
          agg_agg_isNull_36_0 = false;
          agg_value_36 = agg_value_37;
          continue;
        }

        if (!agg_bufIsNull_1) {
          agg_agg_isNull_36_0 = false;
          agg_value_36 = agg_bufValue_1;
          continue;
        }

      } while (false);
// update aggregation buffer
      agg_bufIsNull_0 = agg_agg_isNull_28_0;
      agg_bufValue_0 = agg_value_28;

      agg_bufIsNull_1 = agg_agg_isNull_36_0;
      agg_bufValue_1 = agg_value_36;

    }

    private void agg_doConsume_0(int agg_expr_0_0, int agg_expr_1_0) throws java.io.IOException {
// do aggregate
// common sub-expressions

// evaluate aggregate function
      agg_agg_isNull_11_0 = true;
      long agg_value_11 = -1L;
      do {
        if (!agg_bufIsNull_2) {
          agg_agg_isNull_11_0 = false;
          agg_value_11 = agg_bufValue_2;
          continue;
        }

        boolean agg_isNull_13 = false;
        long agg_value_13 = -1L;
        if (!false) {
          agg_value_13 = (long) 0;
        }
        if (!agg_isNull_13) {
          agg_agg_isNull_11_0 = false;
          agg_value_11 = agg_value_13;
          continue;
        }

      } while (false);
      boolean agg_isNull_15 = false;
      long agg_value_15 = -1L;
      if (!false) {
        agg_value_15 = (long) agg_expr_0_0;
      }
      long agg_value_10 = -1L;

      agg_value_10 = agg_value_11 + agg_value_15;
      agg_agg_isNull_18_0 = true;
      long agg_value_18 = -1L;
      do {
        if (!agg_bufIsNull_3) {
          agg_agg_isNull_18_0 = false;
          agg_value_18 = agg_bufValue_3;
          continue;
        }

        boolean agg_isNull_20 = false;
        long agg_value_20 = -1L;
        if (!false) {
          agg_value_20 = (long) 0;
        }
        if (!agg_isNull_20) {
          agg_agg_isNull_18_0 = false;
          agg_value_18 = agg_value_20;
          continue;
        }

      } while (false);
      boolean agg_isNull_22 = false;
      long agg_value_22 = -1L;
      if (!false) {
        agg_value_22 = (long) agg_expr_1_0;
      }
      long agg_value_17 = -1L;

      agg_value_17 = agg_value_18 + agg_value_22;
// update aggregation buffer
      agg_bufIsNull_2 = false;
      agg_bufValue_2 = agg_value_10;

      agg_bufIsNull_3 = false;
      agg_bufValue_3 = agg_value_17;

    }

    private void agg_doAggregateWithoutKey_1() throws java.io.IOException {
// initialize aggregation buffer
      agg_bufIsNull_2 = true;
      agg_bufValue_2 = -1L;
      agg_bufIsNull_3 = true;
      agg_bufValue_3 = -1L;

      while ( dynamiclocaltablescan_input_0.hasNext()) {
        InternalRow dynamiclocaltablescan_row_0 = (InternalRow) dynamiclocaltablescan_input_0.next();
        ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
        do {
          int dynamiclocaltablescan_value_1 = dynamiclocaltablescan_row_0.getInt(1);

          boolean filter_isNull_0 = true;
          boolean filter_value_0 = false;

          if (!false) {
            filter_isNull_0 = false; // resultCode could change nullability.
            filter_value_0 = dynamiclocaltablescan_value_1 < 5;

          }
          if (filter_isNull_0 || !filter_value_0) continue;

          ((org.apache.spark.sql.execution.metric.SQLMetric) references[1] /* numOutputRows */).add(1);

          int dynamiclocaltablescan_value_2 = dynamiclocaltablescan_row_0.getInt(2);

          agg_doConsume_0(dynamiclocaltablescan_value_1, dynamiclocaltablescan_value_2);

        } while(false);
// shouldStop check is eliminated
      }

    }

    protected void processNext() throws java.io.IOException {
      while (!agg_initAgg_0) {
        agg_initAgg_0 = true;
        long agg_beforeAgg_1 = System.nanoTime();
        agg_doAggregateWithoutKey_0();
        ((org.apache.spark.sql.execution.metric.SQLMetric) references[5] /* aggTime */).add((System.nanoTime() - agg_beforeAgg_1) / 1000000);

// output the result

        ((org.apache.spark.sql.execution.metric.SQLMetric) references[4] /* numOutputRows */).add(1);
        filter_mutableStateArray_0[5].reset();

        filter_mutableStateArray_0[5].zeroOutNullBytes();

        if (agg_bufIsNull_0) {
          filter_mutableStateArray_0[5].setNullAt(0);
        } else {
          filter_mutableStateArray_0[5].write(0, agg_bufValue_0);
        }

        if (agg_bufIsNull_1) {
          filter_mutableStateArray_0[5].setNullAt(1);
        } else {
          filter_mutableStateArray_0[5].write(1, agg_bufValue_1);
        }
        append((filter_mutableStateArray_0[5].getRow()));
      }
    }

  }
}

// End GeneratedIteratorForCodegenStage1.java
