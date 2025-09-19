package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class KafkaTransactionListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaTransactionListener.class);

    private final List<Transaction> receivedTransactions = new CopyOnWriteArrayList<>();

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void listen(Transaction transaction) {
        receivedTransactions.add(transaction);
        log.info("Received transaction: {}", transaction);
    }

    public List<Transaction> getReceivedTransactions() {
        return receivedTransactions;
    }
}