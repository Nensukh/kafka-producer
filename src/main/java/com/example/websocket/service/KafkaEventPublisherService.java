package com.example.websocket.service;

import com.example.websocket.model.PaymentEvent;
import com.example.websocket.util.SamplePaymentEventGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaEventPublisherService {

    @Autowired
    KafkaTemplate<String, PaymentEvent> KafkaJsontemplate;
    @Autowired
    SamplePaymentEventGenerator samplePaymentEventGenerator;
    String TOPIC_NAME = "payment-events";


    public void publishPaymentEvent(PaymentEvent paymentEvent){
        paymentEvent.setTxnId(++SamplePaymentEventGenerator.txn_id);
        KafkaJsontemplate.send(TOPIC_NAME,paymentEvent);
    }

    public void publishPaymentEvent(int numberOfRecords) {
        for(int i =0; i< numberOfRecords; i++){
            KafkaJsontemplate.send(TOPIC_NAME,samplePaymentEventGenerator.generatePaymentEvent());
        }
    }
}
