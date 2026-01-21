package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {
        "listeners=PLAINTEXT://localhost:9092",
        "port=9092"
})
public class TaskThreeTests {

    static final Logger logger = LoggerFactory.getLogger(TaskThreeTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Test
    void task_three_verifier() throws InterruptedException {
        // populate test users
        userPopulator.populate();

        // load transaction lines
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");

        // store sent transactions for debugging
        List<String> sentTransactions = new ArrayList<>();

        // send transactions via Kafka
        for (String transactionLine : transactionLines) {
            String transaction = transactionLine;
            kafkaProducer.send(transaction);
            sentTransactions.add(transaction); // inspect this in debugger
        }

        // wait for transactions to be processed
        Thread.sleep(2000);

        logger.info("----------------------------------------------------------");
        logger.info("use your debugger or logs to check Waldorf's balance");

        // fetch Waldorf from DB
        Optional<UserRecord> waldorfOpt = userRepository.findByUsername("waldorf");

        if (waldorfOpt.isPresent()) {
            UserRecord waldorf = waldorfOpt.get();
            logger.info("Waldorf's balance after all transactions: {}", waldorf.getBalance());
        } else {
            logger.warn("User 'waldorf' not found in database!");
        }

        // optional: wait a bit to allow debugger inspection
        Thread.sleep(5000);
    }
}
