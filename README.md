Consumer
```
@Import(QueueConfig.class)
public class MyConsumer {
    @QueueConsumer(topics = "${kafka.topic}", groupId = "${kafka.consumerGroupId}")
    public void consumeKafkaMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String key = record.key();
        String value = record.value();
        String topic = record.topic();
        long offset = record.offset();
        int partition = record.partition();

        log.info("Received message: Key=" + key + ", Value=" + value + ", Offset=" + offset + ", Partition=" + partition + ", Topic=" + topic);

        ack.acknowledge();
    }
}
```
Producer
```
public class MyProducer extends KafkaProducer {
    @Value("${kafka.topic}")
    String topic;

    @Override
    public String getTopic() {
        return topic;
    }
}
```
Methods to be used for publishing
```
myProducer.publishToTopic(String key, T object); //to send an object
myProducer.publishToTopicSync(String key, T object); //to send an object synchronously
myProducer.publishToTopic(String key, String message); //to send a string
myProducer.publishToTopicSync(String key, String object); //to send a string asynchronously
myProducer.publishToTopicSync(String key, String object, Map<String, String> headerMap) //to send an object synchronously with headers
myProducer.publishToTopic(String key, String object, Map<String, String> headerMap) //to send an object asynchronously with headers
```
Configuration : Please provide following properties in the application.yml file
```
bootStrapServer: <kafka bootstrapserver url>
offsetStrategy: latest or earliest
topic: <your-topic-name>
groupId: <consumer-groupId>
```
