package com.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.message.Product;
import org.apache.commons.lang.time.DateUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProductsProducer {

    public static void main(String[] args) throws JsonProcessingException, ExecutionException, InterruptedException {

        ObjectMapper objectMapper = new ObjectMapper();

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("acks","all");

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(kafkaProps);
        Product product = new Product();
        product.setExpiry(new Date());
        product.setManufacturer("anonymous");
        product.setDescription("testing");
        product.setExpiry(DateUtils.addDays(new Date(),180));
        product.setName("streamer");
        for(int i=0;i<2000;i++) {

            product.setId("i"+i);
            if ( i%2 == 0) {
                product.setCategory("fashion");
            } else {
                product.setCategory("food");
            }
            product.setPrice(""+i);
            String val = objectMapper.writeValueAsString(product);
            System.out.println(val);
            ProducerRecord<String,String> record = new ProducerRecord<String, String>("PRODUCTS","my-key"+i,val);
            RecordMetadata metadata = producer.send(record).get();
        }
        System.out.println("done");

    }
}
