package hw2;

import java.util.*;


import org.apache.ignite.*;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Kafka and Ignite class
 * */
public class ignite {

    /**
     * CONST for settings kafka and ignite
     * */
    private final static String TOPIC = "myTopic";
    private final static String CASHE = "myCashe";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String IGNITE_CONFIG = "/home/cloudera/Desktop/david/igniteconfig.xml";
    private final static String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private final static String INTEGER_DESERIALIZER = "org.apache.kafka.common.serialization.IntegerDeserializer";


    /**
     * Function consumer
     * read from kafka, write to ignite cache
     * */
    public void consumer()
    {
        Ignition.setClientMode(true);
        Ignite ignite = Ignition.start(IGNITE_CONFIG);
        ignite.active(true);


        CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<>(CASHE);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setBackups(1);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setIndexedTypes(Integer.class, String.class);

        IgniteDataStreamer<Integer, String> streamer = ignite.dataStreamer(CASHE);

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(getConsumerProps());

        consumer.subscribe(Collections.singletonList(TOPIC));
        ConsumerRecords<Integer, String> records = consumer.poll(1000);

        for (ConsumerRecord<Integer, String> record : records) {
            //System.out.println(record.key().toString()+" "+record.value());

            streamer.addData(record.key(), record.value());
        }
        consumer.commitSync();

        streamer.flush();
    }

    /**
     * Function compute. Read from ignite cache and cumpute data
     * */
    public void compute()
    {

        Ignition.setClientMode(true);
        Ignite ignite = Ignition.ignite();
        //ignite.active(true);
        CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<>(CASHE);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setBackups(1);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setIndexedTypes(Integer.class, String.class);

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cacheCfg);
        if (cache.size() != 0) {

            for(int i=0; i<cache.size();i++) {
                set_result(cache.get(i));
            }


            result();

        }
        ignite.close();

    }

    /**
     * Temporary arraydata for result
     * Save data from ignite and calculate exists metrics
     * */
    private ArrayList <String[]> list = new ArrayList<>();
    private List<String> metrics = new ArrayList<>();

    private void set_result(String cachitem)
    {
        String[] linedata = cachitem.split(",");
        list.add(linedata);
        if(metrics.indexOf(linedata[0])==-1) {
            metrics.add(linedata[0]);
        }
    }


    /**
     * Function calculate and print result
     * */
    private String result()
    {
        String result ="";
        for (String metric:metrics) {
            //System.out.println("Metric "+metric);
            int count = 0;
            double sum = 0.0;
            String time = "10000000000";
            for (String[] itemlist:list) {


                if(itemlist[0].equals(metric))
                {
                    count++;
                    sum+=Float.parseFloat(itemlist[2]);
                    if(Long.parseLong(itemlist[1])<Long.parseLong(time))
                    {
                        time = itemlist[1];
                    }
                }

            }

            result+=metric+","+time+","+count+"m,"+(sum/count)+"\n";


            count = 0;
            sum = 0.0;
            time = "";
        }
        System.out.println(result);
        return result;
    }


    /**
     * Set properties consumer kafka
     * */
    static public Properties getConsumerProps(){
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.deserializer", INTEGER_DESERIALIZER);
        props.put("value.deserializer", STRING_DESERIALIZER);
        props.put("group.id", "group");
        return props;
    }
}
