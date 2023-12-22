package log.anomalies;

import org.apache.kafka.clients.producer.*;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class HealthAppProducer {
    private static final Logger log = LoggerFactory.getLogger(HealthAppProducer.class.getSimpleName());
    // HealthApp log fields to be used as keys in JSON objects
    private static final String[] KEYS = {"Time", "Component", "PID", "Content", "EventID", "EventTemplate"};

    // Method for creating and returning Kafka producer
    private static KafkaProducer<String, String> createKafkaProducer(String configFilepath){
        // custom class for reading in data from Kafka config file
        KafkaConfig kc = new KafkaConfig(configFilepath);

        //  define properties for connecting to Kafka cluster
        var props = new Properties();
        props.put("bootstrap.servers", kc.getBootstrapServer());
        props.put("sasl.mechanism", kc.getSaslMechanism());
        props.put("security.protocol", kc.getSecurityProtocol());
        props.put("sasl.jaas.config", kc.getSaslJaasConfig());

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // set file compression in producer for efficient batch processing, increase linger  to 20ms
        // and batch size to 32kb to send larger compressed messages
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        return new KafkaProducer<String, String>(props);
    }

    public static void main(String[] args) {
        String healthLogsFilepath = "src/main/resources/HealthApp.log";
        String configFilepath = "src/main/resources/kafka_config.txt";
        String topic = "health_app";
        KafkaProducer<String, String> healthAppProducer = createKafkaProducer(configFilepath);

        try {
            File healthAppFile = new File(healthLogsFilepath);
            FileReader healthAppFR = new FileReader(healthAppFile);
            BufferedReader healthAppBR = new BufferedReader(healthAppFR);
            String line;

            // read in file, parse every log line and create JSON object
            while ((line = healthAppBR.readLine()) != null) {
                String[] vals = line.split("\\|");
                JSONObject logJSON = new JSONObject();

                // iterate through vals array, use KEYS array to put key:value pairs into logJSON object
                int i = 0;
                for (String val : vals) {
                    // a few entries have extra fields, ignoring extra fields with break clause
                    if(i > 3) { break; }

                    logJSON.put(KEYS[i], val);
                    i++;
                }
                //  create and send producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, logJSON.toString());

                healthAppProducer.send(producerRecord, new Callback() {
                    //executes when record is sent successfully, or throws exception
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e == null) {
                            log.info("Received metadata:" +
                                    "\nTopic: " + metadata.topic() +
                                    "\nPartition: " + metadata.partition() +
                                    "\nOffset: " + metadata.offset() +
                                    "\nTimestamp: " + metadata.timestamp()
                            );
                        } else {
                            log.error("Producer error: ", e);
                        }
                    }
                });
            }
        } catch (FileNotFoundException e) {
            log.error("File not found", e);
        } catch (IOException e) {
            log.error("IO exception", e);
        } catch (Exception e) {
            log.error("Unexpected error", e);
        } finally{
            // flush and close producer, producer.flush() to flush without closing
            healthAppProducer.close();
        }
    }
}
