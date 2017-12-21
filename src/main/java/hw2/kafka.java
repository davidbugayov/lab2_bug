package hw2;

import java.util.Properties;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Kafka Class
 * */
public class kafka {

    /**
     * CONST for settings kafka
     * */
    private final static String TOPIC = "myTopic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private final static String INTEGER_SERIALIZER = "org.apache.kafka.common.serialization.IntegerSerializer";

    /**
     * Real path source file
     * */
    private static String path = "";

    private static String filecontent = "";
    /**
     * Construct class kafka
     * @param pathfile
     *
     * */
    public kafka(String pathfile) {

        this.path = pathfile;

        try (BufferedReader br = new BufferedReader(new FileReader(this.path))) {
            String line;

            while ((line = br.readLine()) != null) {
                filecontent+=line;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Run process kafka
     * */
    public int run()
    {
        Producer<Integer, String> producer = new KafkaProducer<>(getProducerProps());

        ProducerCallback callback = new ProducerCallback();

        int counter = 0;

        try {

            String[] linecontent = filecontent.split("\n");

            for (String line:linecontent) {

                String[] linedata = line.split(",");
                String lineresult = String.format("%s,%s,%s", linedata[0],linedata[1],linedata[2]);

                ProducerRecord<Integer, String> record = new ProducerRecord<>(TOPIC, counter, lineresult);
                producer.send(record, callback);

                counter++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            producer.close();
        }
        return counter;
    }

    /**
     * Callback function kafka for debug
     * */
    private static class ProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error while producing message to topic :" + recordMetadata);
                e.printStackTrace();
            } else {
                String message = String.format("sent message to topic:%s partition:%s  offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
               // System.out.println(message);
            }
        }
    }


    /**
     * Set properties producer kafka
     * */
    static public Properties getProducerProps(){
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", INTEGER_SERIALIZER);
        props.put("value.serializer", STRING_SERIALIZER);
        return props;
    }

}
