package simple.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author:lcp
 * @CreateTime:2020-10-21 09:02:52
 * @Mark:
 **/
public class kafkaSource {
    private static Properties properties = new Properties();
    private static final String BOOTSTRAPLIST = "hdp1:6667,hdp2:6667";
    private static final String GROUPID = "metric-group";
    private static final String TOPIC = "metric";

    static {
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAPLIST);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer<String>(TOPIC, new SimpleStringSchema(), properties));

        kafkaSource.print();

        env.execute();
    }
}
