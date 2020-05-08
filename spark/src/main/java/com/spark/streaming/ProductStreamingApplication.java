package com.spark.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.message.Product;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import static org.apache.spark.sql.functions.*;
import java.util.*;

public class ProductStreamingApplication {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws InterruptedException {

        System.setProperty("spark.driver.host","localhost");

        SparkConf conf = new SparkConf().setAppName("products").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.minutes(1l));

        Map<String,Object> kafkaProps = new HashMap<String, Object>();
        kafkaProps.put("bootstrap.servers","localhost:9092");
        kafkaProps.put("key.deserializer", StringDeserializer.class);
        kafkaProps.put("value.deserializer", StringDeserializer.class);
        kafkaProps.put("group.id", "streaming");
        kafkaProps.put("auto.offset.reset", "latest");

        JavaInputDStream<ConsumerRecord<String,String>> productStream = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(Arrays.asList("PRODUCTS"),kafkaProps));

        JavaDStream<Product> products = productStream.map(new Function<ConsumerRecord<String, String>, Product>() {
            public Product call(ConsumerRecord<String, String> v1) throws Exception {
                return MAPPER.readValue(v1.value(),Product.class);
            }
        });

        final SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        String schema = "name description id price manufacturer category";

        List<StructField> structFields = new ArrayList<StructField>();
        for (String name : schema.split(" ")) {
            StructField field = DataTypes.createStructField(name,DataTypes.StringType,true);
            structFields.add(field);
        }

        final StructType structSchema = DataTypes.createStructType(structFields);

        products.foreachRDD(new VoidFunction<JavaRDD<Product>>() {
            public void call(JavaRDD<Product> productJavaRDD) throws Exception {

                JavaRDD<Row> rows=productJavaRDD.map(new Function<Product, Row>() {
                    public Row call(Product v1) throws Exception {
                        Row row = RowFactory.create(v1.getName(),v1.getDescription(),v1.getId(),v1.getPrice(),v1.getManufacturer(),v1.getCategory());
                        return row;
                    }
                });

                Dataset<Row> productDataSet = sparkSession.createDataFrame(rows,structSchema);

                productDataSet.show(10);

                Dataset<Row> categories = productDataSet.groupBy(col("category")).count();
                categories.show();

                productDataSet.agg(avg("price")).show();

                productDataSet.agg(max("price"));
            }
        });

        jsc.start();

        jsc.awaitTermination();
    }
}
