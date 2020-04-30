package example;

import example.avro.Sample;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

public class TestDeepCopy {

    @Test
    void avroObjectCopy() {
        Sample s1 = Sample.newBuilder()
                .setPrice(BigDecimal.valueOf(42.32))
                .setId("A12345")

                .build();

        // This succeeds, with AVRO 1.8.2+
        Sample s2 = Sample.newBuilder(s1).build();
    }

    @Test
    void copyWithFlinkAvroSerializer() {
        AvroSerializer<Sample> serializer = new AvroSerializer<>(Sample.class);

        Sample s1 = Sample.newBuilder()
                .setPrice(BigDecimal.valueOf(42.32))
                .setId("A12345")
                .build();

        // This fails with Flink 1.10.0 throwing
        // java.lang.ClassCastException: java.math.BigDecimal cannot be cast to java.nio.ByteBuffer
        //          at org.apache.avro.generic.GenericData.deepCopyRaw(GenericData.java:1248)
        //          at org.apache.avro.generic.GenericData.deepCopy(GenericData.java:1224)
        //          ...
        Sample s2 = serializer.copy(s1);
    }
}
