package com.dolphs.payment.repository;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;

@Configuration
@ImportRuntimeHints(AppRuntimeHints.class)
public class HazelcastConfiguration {

    @Bean
    public HazelcastInstance hazelcastInstance(@Value("${hazelcast.url}") String hazelCastUrl) {
        // Create a Hazelcast configuration
        ClientConfig config = new ClientConfig();
        config.setClusterName("payment-service-cluster");

        config.getSerializationConfig()
                .getCompactSerializationConfig()
                .addClass(Aggregator.class)
                .addSerializer(new TransactionAggregatorSerializer())
                .addSerializer(new TransactionSerializer());

        config.getNetworkConfig().addAddress(hazelCastUrl);

        // Configure the network settings
//        NetworkConfig networkConfig = config.getNetworkConfig();
//        networkConfig.setPort(5701);
//        networkConfig.setPortAutoIncrement(true);
//
//        JoinConfig join = networkConfig.getJoin();
//        join.getMulticastConfig().setEnabled(false);
//        join.getTcpIpConfig()
//                .addMember("machine1").setEnabled(true)
//                .addMember("localhost").setEnabled(true);
//
//        // Enable Hazelcast Client
//        config.setProperty("hazelcast.client", "true");

        // Return the Hazelcast instance
        return HazelcastClient.newHazelcastClient(config);
    }
}
