package com.example.websocket.model;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Getter
@Setter
public class PaymentEvent {

    long txnId;

    String paymentType;

    double amount;

    String accountNumber;

    String customerName;

    Date txnDateTime;

}
