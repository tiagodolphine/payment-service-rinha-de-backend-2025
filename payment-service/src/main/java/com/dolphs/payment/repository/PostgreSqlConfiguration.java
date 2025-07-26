package com.dolphs.payment.repository;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class PostgreSqlConfiguration {

    @Value("${spring.r2dbc.host}")
    private String host;

    @Value("${spring.r2dbc.port}")
    private int port;

    @Value("${spring.r2dbc.username}")
    private String username;

    @Value("${spring.r2dbc.password}")
    private String password;

    @Value("${spring.r2dbc.database}")
    private String database;

    @Value("${spring.r2dbc.pool.max-size}")
    private Integer maxConnections;

    @Value("${spring.r2dbc.pool.initial-size}")
    private Integer minConnections;

    @Bean
    @Primary
    public ConnectionPool connectionFactory() {
        Map<String, String> options = new HashMap<>();
        options.put("lock_timeout", "3s");
        //options.put("statement_timeout", "5m");
        var connectionFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host(host)
                .port(port)
                .username(username)
                .password(password)
                .database(database)
                .options(options)
                .build());


        // Create a ConnectionPool for connectionFactory
        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
                .maxIdleTime(Duration.ofMillis(1000))
                .maxSize(maxConnections)
                .initialSize(minConnections)
                .build();

        ConnectionPool pool = new ConnectionPool(configuration);
        return pool;
    }

    @Bean
    public ConnectionPool connectionFactory2() {
        Map<String, String> options = new HashMap<>();
        options.put("lock_timeout", "3s");
        //options.put("statement_timeout", "5m");
        var connectionFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host(host)
                .port(port)
                .username(username)
                .password(password)
                .database(database)
                .options(options)
                .build());

        // Create a ConnectionPool for connectionFactory
        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
                .maxIdleTime(Duration.ofMillis(1000))
                .maxSize(maxConnections)
                .initialSize(minConnections)
                .build();

        ConnectionPool pool = new ConnectionPool(configuration);
        return pool;
    }
}