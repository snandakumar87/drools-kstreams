package com.redhat.demo.dm.ccfraud;

import com.redhat.demo.dm.ccfraud.drools.DroolsRulesApplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.StreamsConfig;


import java.util.Properties;

public class KafkaStreamsRunner {

    private KafkaStreamsRunner() {

    }

    static Properties props = new Properties();


    public static KafkaStreams runKafkaStream() {
        props.put("bootstrap.servers", "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "consumer");

        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        DroolsRulesApplier rulesApplier = new DroolsRulesApplier("");
        KStreamBuilder builder = new KStreamBuilder();

        String inputTopic = props.getProperty("inputTopic");
        String outputTopic = props.getProperty("outputTopic");
        KStream<byte[], String> inputData = builder.stream(inputTopic);
        KStream<byte[], String> outputData = inputData.mapValues(rulesApplier::processTransaction);
        outputData.to(outputTopic);

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return streams;
    }


}
