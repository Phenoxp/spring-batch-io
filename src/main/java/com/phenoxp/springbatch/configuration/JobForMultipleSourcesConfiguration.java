package com.phenoxp.springbatch.configuration;

import com.phenoxp.springbatch.domain.Customer;
import com.phenoxp.springbatch.domain.CustomerFieldSetMapper;
import com.phenoxp.springbatch.domain.CustomerForMultipleSources;
import com.phenoxp.springbatch.domain.CustomerForMultipleSourcesFielSetMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

@Configuration
public class JobForMultipleSourcesConfiguration {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Value("classpath*:/data/customer*.csv")
    private Resource[] inputFiles;

    @Bean
    public MultiResourceItemReader<CustomerForMultipleSources> mutiResourceItemReader() {
        MultiResourceItemReader<CustomerForMultipleSources> reader = new MultiResourceItemReader<>();
        reader.setDelegate(customerItemReader());
        reader.setResources(inputFiles);

        return reader;
    }

    @Bean
    public FlatFileItemReader<CustomerForMultipleSources> customerItemReader() {
        FlatFileItemReader<CustomerForMultipleSources> reader = new FlatFileItemReader<>();

        reader.setLinesToSkip(1);

        DefaultLineMapper<CustomerForMultipleSources> customerLineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames(new String[]{"id", "firstName", "lastName", "birthDate"});

        customerLineMapper.setLineTokenizer(tokenizer);
        customerLineMapper.setFieldSetMapper(new CustomerForMultipleSourcesFielSetMapper());
        customerLineMapper.afterPropertiesSet();

        reader.setLineMapper(customerLineMapper);

        return reader;
    }

    @Bean
    public ItemWriter<CustomerForMultipleSources> customerItemWriter() {
        return items -> {
            items.forEach(System.out::println);
        };
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<CustomerForMultipleSources, CustomerForMultipleSources>chunk(10)
                .reader(mutiResourceItemReader())
                .writer(customerItemWriter())
                .build();
    }

    @Bean
    public Job job() {
        return jobBuilderFactory.get("jobMultiSources")
                .start(step1())
                .build();
    }
}
