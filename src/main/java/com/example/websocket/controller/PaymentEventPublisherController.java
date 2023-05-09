package com.example.websocket.controller;


import com.example.websocket.model.PaymentEvent;
import com.example.websocket.service.KafkaEventPublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
public class PaymentEventPublisherController {

    @Autowired
    KafkaEventPublisherService kafkaEventPublisherService;
    @PostMapping(value = "/publish-payment-event")
    public String publishPaymentEvent(@RequestBody PaymentEvent paymentEvent){
        kafkaEventPublisherService.publishPaymentEvent(paymentEvent);
        return "Message published successfully";
    }

    @PostMapping(value = "/publish-mock-payment-event")
    public String publishMockPaymentEvent(@RequestParam int numberOfRecords){
        long t1 = new Date().getTime();
        kafkaEventPublisherService.publishPaymentEvent(numberOfRecords);
        long t2 = new Date().getTime();
        return  numberOfRecords+ " : Payment events published successfully in" + (t2-t1)/1000 + " seconds";
    }
}
