package com.phenoxp.springbatch.domain;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

import java.time.ZoneId;

public class CustomerForMultipleSourcesFielSetMapper implements FieldSetMapper<CustomerForMultipleSources> {

    @Override
    public CustomerForMultipleSources mapFieldSet(FieldSet fieldSet) throws BindException {
        return CustomerForMultipleSources.builder()
                .id(fieldSet.readLong("id"))
                .firstName(fieldSet.readString("firstName"))
                .lastName(fieldSet.readString("lastName"))
                .birthDate(fieldSet.readDate("birthDate").toInstant().atZone(ZoneId.systemDefault()).toLocalDate())
                .build();
    }
}
