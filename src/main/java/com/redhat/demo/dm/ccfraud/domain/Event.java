package com.redhat.demo.dm.ccfraud.domain;

public class Event {

    private Customer customer;
    private Account account;


    public Customer getCustomer() {
        return customer;
    }

    public void setCustomer(Customer customer) {
        this.customer = customer;
    }

    public Account getAccount() {
        return account;
    }

    public void setAccount(Account account) {
        this.account = account;
    }
}
