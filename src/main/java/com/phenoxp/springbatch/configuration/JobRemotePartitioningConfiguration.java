package com.phenoxp.springbatch.configuration;

import com.phenoxp.springbatch.domain.ColumnRangePartitioner;
import com.phenoxp.springbatch.domain.Customer;
import com.phenoxp.springbatch.domain.CustomerRowMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.partition.BeanFactoryStepLocator;
import org.springframework.batch.integration.partition.MessageChannelPartitionHandler;
import org.springframework.batch.integration.partition.StepExecutionRequestHandler;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.scheduling.support.PeriodicTrigger;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

import static com.phenoxp.springbatch.configuration.ConfigurationUtils.getCustomerJdbcBatchItemWriter;

@Configuration
public class JobRemotePartitioningConfiguration implements ApplicationContextAware {
    private static final int GRID_SIZE = 4;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private JobRepository jobRepository;

    private ApplicationContext applicationContext;

    @Bean
    public PartitionHandler partitionHandler(MessagingTemplate messagingTemplate) throws Exception {
        MessageChannelPartitionHandler partitionHandler = new MessageChannelPartitionHandler();

        partitionHandler.setStepName("slaveStep");
        partitionHandler.setGridSize(GRID_SIZE);
        partitionHandler.setMessagingOperations(messagingTemplate);
        partitionHandler.setPollInterval(5000l);
        partitionHandler.setJobExplorer(jobExplorer);
        partitionHandler.afterPropertiesSet();

        return partitionHandler;
    }


    @Bean
    public ColumnRangePartitioner partitioner() {
        ColumnRangePartitioner columnPartitioner = new ColumnRangePartitioner();

        columnPartitioner.setColumn("id");
        columnPartitioner.setDataSource(dataSource);
        columnPartitioner.setTable("customer");

        return columnPartitioner;
    }

    @Bean
    @Profile("slave")
    @ServiceActivator(inputChannel = "inboundRequests", outputChannel = "outboundStaging")
    public StepExecutionRequestHandler stepExecutionRequestHandler() {
        BeanFactoryStepLocator stepLocator = new BeanFactoryStepLocator();
        stepLocator.setBeanFactory(applicationContext);

        StepExecutionRequestHandler stepExecutionRequestHandler = new StepExecutionRequestHandler();
        stepExecutionRequestHandler.setStepLocator(stepLocator);
        stepExecutionRequestHandler.setJobExplorer(jobExplorer);

        return stepExecutionRequestHandler;
    }

    @Bean(name = PollerMetadata.DEFAULT_POLLER)
    public PollerMetadata defaultPoller() {
        PollerMetadata pollerMetadata = new PollerMetadata();
        pollerMetadata.setTrigger(new PeriodicTrigger(10));

        return pollerMetadata;
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
        return stepBuilderFactory.get("stepRemotePartitioning")
                .partitioner(slaveStep().getName(), partitioner())
                .step(slaveStep())
                .partitionHandler(partitionHandler(null))
                .build();
    }

    @Bean
    @Profile("master")
    public Job job() throws Exception {
        return jobBuilderFactory.get("jobRemotePartioning")
                .start(step1())
                .build();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
