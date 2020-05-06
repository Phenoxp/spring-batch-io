package com.phenoxp.springbatch.configuration;

import com.phenoxp.springbatch.domain.ColumnRangePartitioner;
import com.phenoxp.springbatch.domain.Customer;
import com.phenoxp.springbatch.domain.CustomerRowMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

import static com.phenoxp.springbatch.configuration.ConfigurationUtils.getCustomerJdbcBatchItemWriter;

@Configuration
public class JobPartitioningConfiguration {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Bean
    public ColumnRangePartitioner partitioner() {
        ColumnRangePartitioner columnPartitioner = new ColumnRangePartitioner();

        columnPartitioner.setColumn("id");
        columnPartitioner.setDataSource(dataSource);
        columnPartitioner.setTable("customer");

        return columnPartitioner;
    }

    @Bean
    @StepScope
    public JdbcPagingItemReader<Customer> pagingItemReader(@Value("#{stepExecutionContext['minValue']}") Long minValue,
                                                           @Value("#{stepExecutionContext['maxValue']}") Long maxValue) {
        System.out.println("Reading " + minValue + " to " + maxValue);
        JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();

        reader.setDataSource(dataSource);
        reader.setFetchSize(1000);
        reader.setRowMapper(new CustomerRowMapper());

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("id, firstName, lastName, birthDate");
        queryProvider.setFromClause("from customer");
        queryProvider.setWhereClause("where id >=" + minValue + " and id <" + maxValue);

        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("id", Order.ASCENDING);

        queryProvider.setSortKeys(sortKeys);

        reader.setQueryProvider(queryProvider);

        return reader;
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Customer> customerItemWriter() {
        return getCustomerJdbcBatchItemWriter(dataSource);
    }

    @Bean
    Step slaveStep() {
        return stepBuilderFactory.get("slaveStep")
                .<Customer, Customer>chunk(1000)
                .reader(pagingItemReader(null, null))
                .writer(customerItemWriter())
                .build();
    }

    @Bean
    public Step step1() throws Exception {
        return stepBuilderFactory.get("step1")
                .partitioner(slaveStep().getName(), partitioner())
                .step(slaveStep())
                .gridSize(4) //Four partitions which goes with 04 threads
                .taskExecutor(new SimpleAsyncTaskExecutor())
                .build();
    }

    @Bean
    public Job job() throws Exception {
        return jobBuilderFactory.get("jobPartioning")
                .start(step1())
                .build();
    }
}
