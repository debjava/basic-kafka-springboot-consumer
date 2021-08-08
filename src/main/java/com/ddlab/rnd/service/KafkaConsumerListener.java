package com.ddlab.rnd.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.ddlab.rnd.entity.Employee;

@Service
public class KafkaConsumerListener {

    @KafkaListener(topics = "emp-kafka-topic-1", groupId = "group_json1",
                    containerFactory = "empKafkaListenerContainerFactory")
    public void listenWithHeaders(@Payload Employee emp,
                                  @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("Received Message: \n" + emp + "\nfrom partition: " + partition);
        System.out.println("Employee Id: " + emp.getId());
        System.out.println("Employee Id: " + emp.getName());
    }

    @KafkaListener(topics = "str-kafka-topic-2", groupId = "group_json1",
                    containerFactory = "strKafkaListenerContainerFactory")
    public void listenStringMsg(@Payload String msg,
                                @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("Received Message: \n" + msg + "\nfrom partition: " + partition);

    }
}
