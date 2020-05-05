package com.phenoxp.springbatch.domain;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.classify.Classifier;

public class CustomerClassifier implements Classifier<Customer, ItemWriter<? super Customer>> {
    private ItemWriter<Customer> evenItemWriter;
    private ItemWriter<Customer> oddItemWriter;

    public CustomerClassifier(StaxEventItemWriter<Customer> eventItemWriter, FlatFileItemWriter<Customer> oddItemWriter) {
        this.evenItemWriter = eventItemWriter;
        this.oddItemWriter = oddItemWriter;
    }


    @Override
    public ItemWriter<? super Customer> classify(Customer customer) {
        return customer.getId() % 2 == 0 ? evenItemWriter : oddItemWriter;
    }
}
