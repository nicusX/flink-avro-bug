package example;

import example.avro.Sample;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Sample Flink application where the problem arises.
 */
public class StreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000);
        env.getConfig().setAutoWatermarkInterval(1000);

        Properties kafkaProp = new Properties();
        kafkaProp.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProp.setProperty("group.id", "my-group");

        ConfluentRegistryAvroDeserializationSchema<Sample> deserializationSchema = ConfluentRegistryAvroDeserializationSchema.forSpecific(Sample.class, "http://localhost:8081");
        DataStream<Sample> stream = env.addSource(
                new FlinkKafkaConsumer<>("topic", deserializationSchema, kafkaProp).setStartFromEarliest())
                .assignTimestampsAndWatermarks(new EventTimeExtractor());

        stream.print();

        env.execute("Example");
    }

    public static class EventTimeExtractor extends BoundedOutOfOrdernessTimestampExtractor<Sample> {
        public EventTimeExtractor() {
            super(Time.seconds(60));
        }

        @Override
        public long extractTimestamp(Sample s) {
            return s.getEventTime().toEpochMilli();
        }
    }
}
