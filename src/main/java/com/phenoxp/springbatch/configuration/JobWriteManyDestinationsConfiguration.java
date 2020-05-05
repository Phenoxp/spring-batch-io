package com.phenoxp.springbatch.configuration;

import com.phenoxp.springbatch.domain.Customer;
import com.phenoxp.springbatch.domain.CustomerClassifier;
import com.phenoxp.springbatch.domain.CustomerLineAggregator;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import javax.sql.DataSource;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static com.phenoxp.springbatch.configuration.ConfigurationUtils.getCustomerJdbcPagingItemReader;

//@Configuration
public class JobWriteManyDestinationsConfiguration {
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
    public FlatFileItemWriter<Customer> jsonItemWriter() throws Exception {
        FlatFileItemWriter<Customer> itemWriter = new FlatFileItemWriter<>();

        itemWriter.setLineAggregator(new CustomerLineAggregator());
        String customerOutputPath = File.createTempFile("customerOutputPath", ".out").getAbsolutePath();
        System.out.println(">>>> Ouput Path: " + customerOutputPath);
        itemWriter.setResource(new FileSystemResource(customerOutputPath));
        itemWriter.afterPropertiesSet();

        return itemWriter;
    }

    @Bean
    public StaxEventItemWriter<Customer> xmlItemWriter() throws Exception {
        XStreamMarshaller marshaller = new XStreamMarshaller();

        Map<String, Class> aliases = new HashMap<>();
        aliases.put("customer", Customer.class);

        marshaller.setAliases(aliases);
        StaxEventItemWriter<Customer> itemWriter = new StaxEventItemWriter<>();

        itemWriter.setRootTagName("customers");
        itemWriter.setMarshaller(marshaller);
        String customerOutputPath = File.createTempFile("customerOutput", ".xml").getAbsolutePath();
        System.out.println(">>>> Ouput Path: " + customerOutputPath);
        itemWriter.setResource(new FileSystemResource(customerOutputPath));

        itemWriter.afterPropertiesSet();

        return itemWriter;
    }

    //Write the same data to jsonfile and xmlfile
//    @Bean
//    public CompositeItemWriter<Customer> itemWriter() throws Exception{
//        List<ItemWriter<? super Customer>> writers = new ArrayList<>();
//
//        writers.add(jsonItemWriter());
//        writers.add(xmlItemWriter());
//
//        CompositeItemWriter<Customer> itemWriter = new CompositeItemWriter<>();
//        itemWriter.setDelegates(writers);
//        itemWriter.afterPropertiesSet();
//
//        return itemWriter;
//    }

    @Bean
    public ClassifierCompositeItemWriter<Customer> itemWriter() throws Exception {
        ClassifierCompositeItemWriter<Customer> itemWriter = new ClassifierCompositeItemWriter<>();

        itemWriter.setClassifier(new CustomerClassifier(xmlItemWriter(), jsonItemWriter()));

        return itemWriter;
    }

//This is for CompositeItemWriter
//    @Bean
//    public Step step1() throws Exception{
//        return stepBuilderFactory.get("step1")
//                .<Customer, Customer>chunk(10)
//                .reader(pagingItemReader())
//                .writer(itemWriter())
//                .build();
//    }

    @Bean
    public Step step1() throws Exception {
        return stepBuilderFactory.get("step1")
                .<Customer, Customer>chunk(10)
                .reader(pagingItemReader())
                .writer(itemWriter())
                .stream(xmlItemWriter())
                .stream(jsonItemWriter())
                .build();
    }

    @Bean
    public Job job() throws Exception {
        return jobBuilderFactory.get("jobManyDestinations")
                .start(step1())
                .build();
    }


}
