package log.anomalies;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class HealthAppConsumer {
    private static final Logger log = LoggerFactory.getLogger(HealthAppConsumer.class.getSimpleName());

    private static RestHighLevelClient createOpenSearchClient(String bonsaiConfigFilepath){
        BonsaiConfig bc = new BonsaiConfig(bonsaiConfigFilepath);
        String connString = bc.getConnString();

        URI connUri = URI.create(connString);
        String[] auth = connUri.getUserInfo().split(":");

        CredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

        RestHighLevelClient rhlc = new RestHighLevelClient(
                RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                        .setHttpClientConfigCallback(
                                httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                        .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));

        return rhlc;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(String kafkaConfigFilepath, String groupID){
        // custom class for reading in data from Kafka config file
        KafkaConfig kc = new KafkaConfig(kafkaConfigFilepath);

        //  connect to upstash server
        var props = new Properties();
        props.put("bootstrap.servers", kc.getBootstrapServer());
        props.put("sasl.mechanism", kc.getSaslMechanism());
        props.put("security.protocol", kc.getSecurityProtocol());
        props.put("sasl.jaas.config", kc.getSaslJaasConfig());

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", groupID);
        props.put("auto.offset.reset", "earliest");

        return new KafkaConsumer<>(props);
    }

    private static void getOrCreateIndex(RestHighLevelClient openSearchClient, String index) throws IOException {
        Boolean indexExists = openSearchClient.indices()
                .exists(new GetIndexRequest(index), RequestOptions.DEFAULT);
        if (!indexExists) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
            openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            log.info("HealthApp index created");
        } else {
            log.info("HealthApp index already exists");
        }
    }

    public static void main(String[] args) throws IOException {
        String bonsaiConfigFilepath = "src/main/resources/bonsai_config.txt";
        String kafkaConfigFilepath = "src/main/resources/kafka_config.txt";
        String topic = "health_app";
        String groupID = "health_app_1";
        String healthAppIndex = "health_time_test";

        //Create open search client
        RestHighLevelClient openSearchClient = createOpenSearchClient(bonsaiConfigFilepath);
        // Create Kafka consumer
        KafkaConsumer<String, String> healthAppConsumer = createKafkaConsumer(kafkaConfigFilepath, groupID);

                //Try block will close openSearch client/consumer on success/fail
        try(openSearchClient; healthAppConsumer) {
            //Create index if does not exist
            getOrCreateIndex(openSearchClient, healthAppIndex);

            //add consumer shutdown hook for graceful shutdown, will throw exception within while loop
            final Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    log.info("Shutdown detected, exiting with consumer.wakeup()");
                    healthAppConsumer.wakeup();
                    //join main thread to allow execution of its code
                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });

            //subscribe to topic
            healthAppConsumer.subscribe(Collections.singleton(topic));

            //poll for data
            while (true) {
                log.info("Polling");
                ConsumerRecords<String, String> records = healthAppConsumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Received " + recordCount + " record(s)");

                // upload data to openSearch, use bulkRequest to optimize throughput
                BulkRequest bulkRequest = new BulkRequest();
                for(ConsumerRecord record: records){
                    try {
                        // create ID based on kafka metadata for idempotent insertions
                        String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                        // create upload request. Builder defines key:values pairs to be inserted under source field
                        JSONObject logJSON = new JSONObject(record.value().toString());
                        XContentBuilder builder = XContentFactory.jsonBuilder();
                        builder.startObject();
                        {
                            builder.field("Time", logJSON.get("Time"));
                            builder.timeField("Component", logJSON.get("Component"));
                            builder.field("PID", logJSON.get("PID"));
                            builder.field("Content", logJSON.get("Content"));
                        }
                        builder.endObject();
                        IndexRequest indexRequest = new IndexRequest(healthAppIndex)
                                .source(builder)
                                .id(id);

                        // add to bulk request
                        bulkRequest.add(indexRequest);
                    } catch(Exception e){
                        log.error("Record bulk append failed: ", e);
                    }
                }

                if(bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info(bulkResponse.getItems().length + " record(s) added.");
                }
            }
        } catch(WakeupException we) {
            log.info("Consumer shutting down");
        } catch (IOException e) {
            e.printStackTrace();
        } catch(Exception e){
            log.error("Unexpected consumer error: ", e);
        } finally {
            //close client/consumer and commit offsets
            healthAppConsumer.close();
            openSearchClient.close();
        }
    }
}
