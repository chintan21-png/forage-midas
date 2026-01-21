package com.jpmc.midascore.entity;

import jakarta.persistence.*;

@Entity
public class UserRecord {

    @Id
    @GeneratedValue()
    private long id;

    @Column(nullable = false)
    private String username;

    @Column(nullable = false)
    private float balance;

    protected UserRecord() {
    }

    public UserRecord(String username, float balance) {
        this.username = username;
        this.balance = balance;
    }

    @Override
    public String toString() {
        return String.format("User[id=%d, name='%s', balance='%f'", id, username, balance);
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return username;
    }

    public float getBalance() {
        return balance;
    }

    public void setBalance(float balance) {
        this.balance = balance;
    }
}
