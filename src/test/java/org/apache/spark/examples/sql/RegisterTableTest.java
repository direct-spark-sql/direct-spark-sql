package org.apache.spark.examples.sql;

import org.apache.spark.sql.DirectDataTable;
import org.apache.spark.sql.DirectSparkSession;
import org.apache.spark.sql.types.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RegisterTableTest {

    @Test
    public void testRegisterByListMap() {
        DirectSparkSession sp = DirectSparkSession.builder().getOrCreate();
        List<Map<String, Object>> maps = new ArrayList<>();
        maps.add(new HashMap<String, Object>() {{
            put("a", "10");
            put("b", "10");
            put("c", 1565748795542L);
            put("d", "23");
            put("e", "2.3");
            put("f", "24.03");
            put("g", "true");
            put("h", "1");
            put("i", "12");
            put("j", 1565748795542L);
            put("k", 2342.23);
        }});
        maps.add(new HashMap<String, Object>() {{
            put("a", 200);
            put("b", 10);
            put("c", "1565748795542");
            put("d", 900000);
            put("e", 9.08);
            put("f", 9.0008f);
            put("h", 12);
            put("i", 23);
            put("j", "1565748795542");
            put("k", "strtest");
        }});
        StructType structType = new StructType(new StructField[]{
                new StructField("a", IntegerType$.MODULE$, true, Metadata.empty()),
                new StructField("b", LongType$.MODULE$, true, Metadata.empty()),
                new StructField("c", TimestampType$.MODULE$, true, Metadata.empty()),
                new StructField("d", DecimalType$.MODULE$.SYSTEM_DEFAULT(), true, Metadata.empty()),
                new StructField("e", FloatType$.MODULE$, true, Metadata.empty()),
                new StructField("f", DoubleType$.MODULE$, true, Metadata.empty()),
                new StructField("g", BooleanType$.MODULE$, true, Metadata.empty()),
                new StructField("h", ShortType$.MODULE$, true, Metadata.empty()),
                new StructField("i", ByteType$.MODULE$, true, Metadata.empty()),
                new StructField("j", DateType$.MODULE$, true, Metadata.empty()),
                new StructField("k", StringType$.MODULE$, true, Metadata.empty())
        });
        sp.registerTempView("hello.world", DirectDataTable.fromJavaMapList(structType, maps));
        DirectDataTable directDataTable = sp.sqlDirectly("select * from hello.world");
        Assert.assertEquals("[10,10,2019-08-14 18:13:15.542,23.000000000000000000,2.3," +
                "24.03,true,1,12,2019-08-14,2342.23],[200,10,2019-08-14 18:13:15.542,900000.00000" +
                "0000000000000,9.08,9.000800132751465,null,12,23,2019-08-14,strtest]",
                directDataTable.data().mkString(","));
    }
}
