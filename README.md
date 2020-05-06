# spring-batch-io
Spring Batch with Spring Boot2, Spring 5 & Java 11

### Running remote Paritioning ###

- One or Two slaves: 
    `java -jar -Dspring.profiles.active=slave target/spring-batch-io-0.0.1-SNAPSHOT.jar`
- Master : 
`java -jar -Dspring.profiles.active=master target/spring-batch-io-0.0.1-SNAPSHOT.jar`

