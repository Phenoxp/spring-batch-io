package com.phenoxp.springbatch.configuration;

import com.phenoxp.springbatch.domain.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import javax.sql.DataSource;

import static com.phenoxp.springbatch.configuration.ConfigurationUtils.getCustomerJdbcBatchItemWriter;
import static com.phenoxp.springbatch.configuration.ConfigurationUtils.getCustomerJdbcPagingItemReader;

//@Configuration
public class JobMultiThreadConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Bean
    public JdbcPagingItemReader<Customer> pagingItemReader() {
        return getCustomerJdbcPagingItemReader(dataSource, 1000);
    }

    @Bean
    public JdbcBatchItemWriter<Customer> customerItemWriter() {
        return getCustomerJdbcBatchItemWriter(dataSource);
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("stepMulti")
                .<Customer, Customer>chunk(1000)
                .reader(pagingItemReader())
                .writer(customerItemWriter())
                .taskExecutor(new SimpleAsyncTaskExecutor())
                .build();
    }

    @Bean
    public Job job() {
        return jobBuilderFactory.get("jobMultiThread4")
                .start(step1())
                .build();
    }
}
