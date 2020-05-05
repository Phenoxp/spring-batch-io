package com.phenoxp.springbatch.processor;

import com.phenoxp.springbatch.domain.Customer;
import org.springframework.batch.item.validator.ValidationException;
import org.springframework.batch.item.validator.Validator;

public class CustomerValidator implements Validator<Customer> {

    @Override
    public void validate(Customer value) throws ValidationException {
        if (value.getFirstName().startsWith("A")) {
            throw new ValidationException("First names beginning with A are invalid: " + value);
        }
    }
}
