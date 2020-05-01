package com.phenoxp.springbatch.configuration;

import com.phenoxp.springbatch.reader.StatelessItemReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class JobConfiguration {
    @Autowired
    StepBuilderFactory stepBuilderFactory;

    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Bean
    public StatelessItemReader statelessItemReader() {
        List<String> data = new ArrayList<>(3);

        data.add("Foo");
        data.add("Bar");
        data.add("Jazz");

        return new StatelessItemReader(data);
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<String, String>chunk(2)
                .reader(statelessItemReader())
                .writer(list -> {
                    for (String currentItem : list)
                        System.out.println("currentItem: " + currentItem);
                }).build();
    }

    @Bean
    public Job interfaceJob() {
        return jobBuilderFactory.get("interfaceJob")
                .start(step1())
                .build();

    }
}
