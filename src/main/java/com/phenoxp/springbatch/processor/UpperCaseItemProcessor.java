package com.phenoxp.springbatch.processor;

import com.phenoxp.springbatch.domain.Customer;
import org.springframework.batch.item.ItemProcessor;

public class UpperCaseItemProcessor implements ItemProcessor<Customer, Customer> {

    @Override
    public Customer process(Customer item) throws Exception {
        return Customer.builder()
                .id(item.getId())
                .firstName(item.getFirstName().toUpperCase())
                .lastName(item.getLastName().toUpperCase())
                .birthDate(item.getBirthDate())
                .build();
    }
}
