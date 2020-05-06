package com.phenoxp.springbatch.configuration;

import com.phenoxp.springbatch.domain.Customer;
import com.phenoxp.springbatch.processor.UpperCaseItemProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;

import static com.phenoxp.springbatch.configuration.ConfigurationUtils.getCustomerJdbcPagingItemReader;
import static com.phenoxp.springbatch.configuration.ConfigurationUtils.getCustomerStaxEventItemWriter;

//@Configuration
public class JobProcessorConfiguration {

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
    public StaxEventItemWriter<Customer> customerItemWriter() throws Exception {
        return getCustomerStaxEventItemWriter();
    }

    @Bean
    public UpperCaseItemProcessor itemProcessor() {
        return new UpperCaseItemProcessor();
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
        return jobBuilderFactory.get("jobProcessor")
                .start(step1())
                .build();
    }
}
