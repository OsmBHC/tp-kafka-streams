package ma.enset;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "text-processing-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Liste de mots interdits
        List<String> bannedWords = Arrays.asList("HACK", "SPAM", "XXX");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputStream = builder.stream("text-input");

        // Traitement + filtrage
        KStream<String, String>[] branches = inputStream
                .mapValues(value -> {
                    if (value == null) return "";
                    String cleaned = value.trim().replaceAll("\\s+", " ").toUpperCase();
                    return cleaned;
                })
                .branch(
                        (key, value) -> { // valide
                            boolean notEmpty = !value.isBlank();
                            boolean notTooLong = value.length() <= 100;
                            boolean noBanned = bannedWords.stream().noneMatch(value::contains);
                            return notEmpty && notTooLong && noBanned;
                        },
                        (key, value) -> true // sinon -> invalide
                );
        KStream<String, String> validStream = branches[0];
        KStream<String, String> invalidStream = branches[1];

        validStream.to("text-clean");
        invalidStream.to("text-dead-letter");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
