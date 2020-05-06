package com.phenoxp.springbatch.configuration;

import com.phenoxp.springbatch.domain.Customer;
import com.phenoxp.springbatch.processor.FilteringItemProcessor;
import com.phenoxp.springbatch.processor.UpperCaseItemProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

import java.util.ArrayList;
import java.util.List;

import static com.phenoxp.springbatch.configuration.ConfigurationUtils.getCustomerFlatFileItemWriter;
import static com.phenoxp.springbatch.configuration.ConfigurationUtils.getCustomerJdbcPagingItemReader;

@Configuration
public class JobCompositeProcessorConfiguration {
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Bean
    public JdbcPagingItemReader<Customer> pagingItemReader() {
        return getCustomerJdbcPagingItemReader(dataSource);
    }

    @Bean
    public FlatFileItemWriter<Customer> customerItemWriter() throws Exception {
        return getCustomerFlatFileItemWriter();
    }

    @Bean
    public CompositeItemProcessor<Customer, Customer> itemProcessor() throws Exception {
        List<ItemProcessor<Customer, Customer>> deleagates = new ArrayList<>(2);

        deleagates.add(new FilteringItemProcessor());
        deleagates.add(new UpperCaseItemProcessor());

        CompositeItemProcessor<Customer, Customer> compositeItemProcessor = new CompositeItemProcessor<>();

        compositeItemProcessor.setDelegates(deleagates);
        compositeItemProcessor.afterPropertiesSet();

        return compositeItemProcessor;
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
        return jobBuilderFactory.get("jobCompositeProcessor")
                .start(step1())
                .build();
    }
}
