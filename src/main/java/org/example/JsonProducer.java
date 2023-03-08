package org.example;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.data.Ride;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import io.github.cdimascio.dotenv.Dotenv;

public class JsonProducer {

    private final Properties props = new Properties();

    public JsonProducer() {
        Dotenv dotenv = Dotenv.configure().load();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-75m1o.europe-west3.gcp.confluent.cloud:9092");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='"
                        + dotenv.get("user")
                        + "' password='"
                        + dotenv.get("password")
                        + "';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");
    }

    public List<Ride> getRides() throws IOException, CsvException {
        var ridesStream = this.getClass().getResource("/rides.csv");
        assert ridesStream != null;
        var reader = new CSVReader(new FileReader(ridesStream.getFile()));
        reader.skip(1);
        return reader
                .readAll()
                .stream()
                .map(Ride::new)
                .collect(Collectors.toList());

    }

    public void publishRides(List<Ride> rides) {
        var kafkaProducer = new KafkaProducer<String, Ride>(props);
        for (Ride ride: rides) {
            System.out.println(ride.PULocationID);
            kafkaProducer.send(new ProducerRecord<>("rides", String.valueOf(ride.PULocationID), ride));
        }
    }

    public static void main(String[] args) throws IOException, CsvException {
        var producer = new JsonProducer();
        var rides = producer.getRides();
        producer.publishRides(rides);
    }
}
