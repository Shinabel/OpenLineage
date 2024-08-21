/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_ACCESS_KEY_ID;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_REGION;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_SECRET_ACCESS_KEY;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_SESSION_TOKEN;

import java.net.URI;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

class TransportFactoryTest {

  @AfterEach
  void clearAWSSystemProperties() {
    System.clearProperty(AWS_REGION.property());
    System.clearProperty(AWS_ACCESS_KEY_ID.property());
    System.clearProperty(AWS_SECRET_ACCESS_KEY.property());
    System.clearProperty(AWS_SESSION_TOKEN.property());
  }

  @Test
  void createsConsoleTransport() {
    TransportConfig config = new ConsoleConfig();
    TransportFactory transportFactory = new TransportFactory(config);

    assertThat(transportFactory.build()).isInstanceOf(ConsoleTransport.class);
  }

  @Test
  void createsHttpTransport() {
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("http://localhost:1500"));
    TransportFactory transportFactory = new TransportFactory(config);

    assertThat(transportFactory.build()).isInstanceOf(HttpTransport.class);
  }

  @Test
  void createsKafkaTransport() {
    KafkaConfig config = new KafkaConfig();
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty(
        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.setProperty(
        "value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    config.setProperties(properties);
    config.setTopicName("test-topic");
    TransportFactory transportFactory = new TransportFactory(config);

    assertThat(transportFactory.build()).isInstanceOf(KafkaTransport.class);
  }

  @Test
  void createAmazonDataZoneTransport() {
    System.setProperty(AWS_REGION.property(), Region.EU_WEST_1.id());
    System.setProperty(AWS_ACCESS_KEY_ID.property(), "AccessKey");
    System.setProperty(AWS_SECRET_ACCESS_KEY.property(), "SecretAccessKey");
    System.setProperty(AWS_SESSION_TOKEN.property(), "AWSSessionToken");

    AmazonDataZoneConfig config = new AmazonDataZoneConfig();
    config.setDomainId("dzd_a1b2c3d4e5f6g7");
    TransportFactory transportFactory = new TransportFactory(config);

    assertThat(transportFactory.build()).isInstanceOf(AmazonDataZoneTransport.class);
  }
}
