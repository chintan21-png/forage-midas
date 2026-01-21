package com.jpmc.midascore.kafka;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class TransactionListener {

    private final List<Transaction> receivedTransactions =
            Collections.synchronizedList(new ArrayList<>());

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    public void listen(Transaction transaction) {
        receivedTransactions.add(transaction);
    }

    public List<Transaction> getReceivedTransactions() {
        return receivedTransactions;
    }
}
