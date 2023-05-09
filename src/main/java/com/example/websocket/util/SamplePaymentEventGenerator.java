package com.example.websocket.util;

import com.example.websocket.model.PaymentEvent;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class SamplePaymentEventGenerator {
    public static long txn_id = 0;
    public PaymentEvent generatePaymentEvent(){
        PaymentEvent paymentEvent = new PaymentEvent();
        int randomNum = ThreadLocalRandom.current().nextInt(100, 1001);
        int flag = ThreadLocalRandom.current().nextInt(1, 3);
        paymentEvent.setAmount(randomNum);
        paymentEvent.setTxnDateTime(new Date());
        String paymentType = flag ==1? "Credit" : "Debit";
        paymentEvent.setPaymentType(paymentType);
        paymentEvent.setTxnId(++txn_id);
        return paymentEvent;
    }
}
