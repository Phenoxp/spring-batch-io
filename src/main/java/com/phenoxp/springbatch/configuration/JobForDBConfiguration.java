package com.phenoxp.springbatch.configuration;

import com.phenoxp.springbatch.domain.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;

import static com.phenoxp.springbatch.configuration.ConfigurationUtils.getCustomerJdbcPagingItemReader;

//@Configuration
public class JobForDBConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

//    Uncomment these lines to use Reading by cursor
//    Also uncomment the commented step1 method
//    @Bean
//    public JdbcCursorItemReader<Customer> cursorItemReader() {
//        JdbcCursorItemReader<Customer> reader = new JdbcCursorItemReader<>();
//
//        //The reason for ordering here is upon restart, I will be able to pick up at the same point
//        reader.setSql("select id, firstName, lastName, birthDate from customer order by lastName, firstName");
//        reader.setDataSource(this.dataSource);
//        reader.setRowMapper(new CustomerRowMapper());
//
//        return reader;
//    }

    @Bean
    public JdbcPagingItemReader<Customer> pagingItemReader(){
        return getCustomerJdbcPagingItemReader(dataSource);
    }

    @Bean
    public ItemWriter<Customer> customerItemWriter() {
        return items -> {
            items.forEach(System.out::println);
        };
    }

//    Uncomment this lines if you are using cursorItemReader()
//    @Bean
//    public Step step1(){
//        return stepBuilderFactory.get("step1")
//                .<Customer, Customer>chunk(10)
//                .reader(cursorItemReader())
//                .writer(customerItemWriter())
//                .build();
//    }
    @Bean
    public Step step1(){
        return stepBuilderFactory.get("step1")
                .<Customer, Customer>chunk(10)
                .reader(pagingItemReader())
                .writer(customerItemWriter())
                .build();
    }

    @Bean
    public Job job(){
        return jobBuilderFactory.get("job")
                .start(step1())
                .build();
    }
}
