package com.phenoxp.springbatch.configuration;

import com.phenoxp.springbatch.domain.Customer;
import com.phenoxp.springbatch.domain.CustomerLineAggregator;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;
import java.io.File;

import static com.phenoxp.springbatch.configuration.ConfigurationUtils.getCustomerJdbcPagingItemReader;

//@Configuration
public class JobWriteFlatFileConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Bean
    public JdbcPagingItemReader<Customer> pagingItemReader() {
        return getCustomerJdbcPagingItemReader(dataSource, 10);
    }

    @Bean
    public FlatFileItemWriter<Customer> customerItemWriter() throws Exception {
        FlatFileItemWriter<Customer> itemWriter = new FlatFileItemWriter<>();
// Default line aggregator of Spring
//        itemWriter.setLineAggregator(new PassThroughLineAggregator<>());
        itemWriter.setLineAggregator(new CustomerLineAggregator());
        String customerOutputPath = File.createTempFile("customerOutput", ".out").getAbsolutePath();
        System.out.println(">>>> Ouput Path: " + customerOutputPath);
        itemWriter.setResource(new FileSystemResource(customerOutputPath));
        itemWriter.afterPropertiesSet();

        return itemWriter;
    }

    @Bean
    public Step step1() throws Exception {
        return stepBuilderFactory.get("step1")
                .<Customer, Customer>chunk(10)
                .reader(pagingItemReader())
                .writer(customerItemWriter())
                .build();
    }

    @Bean
    public Job job() throws Exception {
        return jobBuilderFactory.get("jobWriteFile")
                .start(step1())
                .build();
    }


}
