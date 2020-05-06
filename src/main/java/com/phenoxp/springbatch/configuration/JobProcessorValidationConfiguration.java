package com.phenoxp.springbatch.configuration;

import com.phenoxp.springbatch.domain.Customer;
import com.phenoxp.springbatch.processor.CustomerValidator;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.validator.ValidatingItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;

import static com.phenoxp.springbatch.configuration.ConfigurationUtils.getCustomerFlatFileItemWriter;
import static com.phenoxp.springbatch.configuration.ConfigurationUtils.getCustomerJdbcPagingItemReader;

//@Configuration
public class JobProcessorValidationConfiguration {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Bean
    public JdbcPagingItemReader<Customer> pagingItemReader() {
        return getCustomerJdbcPagingItemReader(dataSource, 10);
    }

    @Bean
    public FlatFileItemWriter<Customer> customerItemWriter() throws Exception {
        return getCustomerFlatFileItemWriter();
    }

    @Bean
    public ValidatingItemProcessor itemProcessor() {
        ValidatingItemProcessor<Customer> customerValidatingItemProcessor = new ValidatingItemProcessor<>(new CustomerValidator());

        //if filter is false, when there is validationException, fail the job
        //otherwise, continue
        customerValidatingItemProcessor.setFilter(true);

        return customerValidatingItemProcessor;
    }


    @Bean
    public Step step1() throws Exception {
        return stepBuilderFactory.get("step1")
                .<Customer, Customer>chunk(10)
                .reader(pagingItemReader())
                .processor(itemProcessor())
                .writer(customerItemWriter())
                .build();
    }

    @Bean
    public Job job() throws Exception {
        return jobBuilderFactory.get("jobValidatingProcessor")
                .start(step1())
                .build();
    }
}
