package com.linkedin.venice.pulsar.sink;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.common.schema.KeyValue;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test for the Venice Pulsar Sink.
 */
public class PulsarVeniceSinkTest {
  private static final Logger LOGGER = LogManager.getLogger(PulsarVeniceSinkTest.class);

  private DockerComposeContainer environment;

  // Generated by delombok
  public static class Person {
    private String name;
    private int age;

    @java.lang.SuppressWarnings("all")
    public String getName() {
      return this.name;
    }

    @java.lang.SuppressWarnings("all")
    public int getAge() {
      return this.age;
    }

    @java.lang.SuppressWarnings("all")
    public void setName(final String name) {
      this.name = name;
    }

    @java.lang.SuppressWarnings("all")
    public void setAge(final int age) {
      this.age = age;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("all")
    public boolean equals(final java.lang.Object o) {
      if (o == this)
        return true;
      if (!(o instanceof PulsarVeniceSinkTest.Person))
        return false;
      final PulsarVeniceSinkTest.Person other = (PulsarVeniceSinkTest.Person) o;
      if (!other.canEqual((java.lang.Object) this))
        return false;
      if (this.getAge() != other.getAge())
        return false;
      final java.lang.Object this$name = this.getName();
      final java.lang.Object other$name = other.getName();
      if (this$name == null ? other$name != null : !this$name.equals(other$name))
        return false;
      return true;
    }

    @java.lang.SuppressWarnings("all")
    protected boolean canEqual(final java.lang.Object other) {
      return other instanceof PulsarVeniceSinkTest.Person;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("all")
    public int hashCode() {
      final int PRIME = 59;
      int result = 1;
      result = result * PRIME + this.getAge();
      final java.lang.Object $name = this.getName();
      result = result * PRIME + ($name == null ? 43 : $name.hashCode());
      return result;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("all")
    public java.lang.String toString() {
      return "PulsarVeniceSinkTest.Person(name=" + this.getName() + ", age=" + this.getAge() + ")";
    }

    @java.lang.SuppressWarnings("all")
    public Person() {
    }

    @java.lang.SuppressWarnings("all")
    public Person(final String name, final int age) {
      this.name = name;
      this.age = age;
    }
  }

  @BeforeClass
  public void setUp() {
    environment =
        new DockerComposeContainer(new File("src/pulsarIntegrationTest/resources/docker-compose-pulsar-venice.yaml"))
            .withExposedService("proxy", 6650)
            .withExposedService("proxy", 8080)
            .withTailChildContainers(true)
            .waitingFor("proxy", Wait.forHttp("/metrics").forStatusCode(200).forPort(8080))
            .withStartupTimeout(Duration.ofSeconds(300));
    environment.start();
    LOGGER.info("Docker environment started");
  }

  @AfterClass
  public void tearDown() {
    if (environment != null) {
      LOGGER.info("Stopping docker environment");
      environment.stop();
      LOGGER.info("Docker environment stopped");
    }
  }

  @Test
  public void testPulsarVeniceSink() throws Exception {
    LOGGER.info("testPulsarVeniceSink");

    Schema<Person> personSchema = Schema.AVRO(Person.class);
    String schema = personSchema.getSchemaInfo().getSchemaDefinition();

    String keyAsvc = "{\"name\": \"key\",\"type\": \"string\"}";
    String valueAsvc = schema;

    String veniceControllerUrl = "http://venice-controller:5555";
    String veniceRouterUrl = "http://venice-router:7777";
    String keyFile = "/tmp/key.asvc";
    String valueFile = "/tmp/value.asvc";
    String jar = "/opt/venice/bin/venice-admin-tool-all.jar";
    String clusterName = "venice-cluster0";
    String storeName = "t1_n1_s1";

    LOGGER.info("Setting up Venice");
    saveKeyValueSchemaFiles(keyAsvc, valueAsvc, keyFile, valueFile);
    createVeniceStore(veniceControllerUrl, keyFile, valueFile, jar, clusterName, storeName);
    updateVeniceStoreQuotas(veniceControllerUrl, jar, clusterName, storeName);
    initVeniceStore(veniceControllerUrl, jar, clusterName, storeName);

    LOGGER.info("Setting up Pulsar");
    createNamespace();
    setRetention();
    createTopics();

    LOGGER.info("Starting up Pulsar Venice Sink");
    String token =
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJzdXBlcnVzZXIifQ.JQpDWJ9oHD743ZyuIw55Qp0bb8xzP6gK0KIWRniF2WnJB1m3v5MsrpfMlmRIlFc3-htWRAFHCc4E0ipj7JU8HjBqLIvVErRseRG-UTM1EprVkj0mk37jXV3ef7gER0KHn9CUKEQPfmTACeKlQ2oV4_qPAZ6HiEt51vzANfZH24vLCIjiOG77Z4s_w2sfgpiodRmhBLFOg_qnQTfGs7TBDWgu4DRoJ6CYZSEcp8q7j8xp_zNVIFGTRjWskocUvedHS9ZsCGZjzuPvRPp19B0VvAjEjtwpa6j7Khvjf4imjp2QHDnZwpCIEp4DSicwM48F5q4k722IdiyTTsVBWy8Cyg";

    String sinkConfig = "{\"veniceDiscoveryUrl\":\"" + veniceControllerUrl + "\"," + "\"veniceRouterUrl\":\""
        + veniceRouterUrl + "\"," + "\"storeName\":\"t1_n1_s1\"}";

    ExecResult createSinkRes = execByServiceAsssertNoStdErr(
        "proxy",
        "bash",
        "-c",
        "/pulsar/bin/pulsar-admin sinks create --tenant t1 --namespace n1 "
            + "--name venice -t venice -i t1/n1/input --sink-config '" + sinkConfig + "'");
    assertTrue(createSinkRes.getStdout().contains("Created successfully"));

    // Awaitility.await().atMost(90, TimeUnit.SECONDS).untilAsserted(() -> {
    // ExecResult res = execByService(
    // "venice-client",
    // "bash",
    // "-c",
    // "java -jar " + jar + " --describe-store --url " + veniceControllerUrl
    // + " --cluster " + clusterName
    // + " --store " + storeName);
    //
    // assertTrue(res.getStdout().contains("\"status\" : \"ONLINE\""));
    // });

    // await() above does not work, issue number https://github.com/linkedin/venice/issues/387
    // Have to sleep to let the Venice store become writable,
    // otherwise Sink's write to venice will not fail but the data won't appear in Venice.
    Thread.sleep(15000);

    PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .authentication(new AuthenticationDisabled())
        .allowTlsInsecureConnection(true)
        .build();
    Producer<KeyValue<String, Person>> destTopicProducer =
        client.newProducer(Schema.KeyValue(Schema.STRING, personSchema))
            .topic("t1/n1/input")
            .blockIfQueueFull(true)
            .create();

    final int numRecordsToSend = 50;
    for (int i = 0; i < numRecordsToSend; i++) {
      LOGGER.info("Sending {}", i);
      Person person = new Person("name" + i, i);
      destTopicProducer.sendAsync(new KeyValue<>(person.getName(), person));
    }

    LOGGER.info("Flush");
    destTopicProducer.flush();
    destTopicProducer.close();
    client.close();

    LOGGER.info("Querying Venice to check messages");
    for (int i = 0; i < numRecordsToSend; i++) {
      String name = "name" + i;
      int expectedAge = i;

      Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
        // StdErr will have some noise like
        // "StatusLogger Log4j2 could not find a logging implementation."
        ExecResult res = execByService(
            "venice-client",
            "bash",
            "-c",
            "java -jar /opt/venice/bin/venice-thin-client-all.jar " + storeName + " " + name + " " + veniceRouterUrl
                + " " + "false" + " " + "\"\"");
        assertTrue(res.getStdout().contains("key=" + name));
        assertFalse(res.getStdout().contains("value=null"));
        assertTrue(res.getStdout().contains("value=" + "{\"age\": " + expectedAge + ", \"name\": \"" + name + "\"}"));
      });
    }

    LOGGER.info("Deleting Pulsar Venice Sink");
    ExecResult deleteSinkRes = execByServiceAsssertNoStdErr(
        "proxy",
        "bash",
        "-c",
        "/pulsar/bin/pulsar-admin sinks delete --tenant t1 --namespace n1 --name venice");
    assertTrue(deleteSinkRes.getStdout().contains("Deleted successfully"));
  }

  private void initVeniceStore(String veniceControllerUrl, String jar, String clusterName, String storeName)
      throws Exception {
    execByServiceAsssertNoStdErr(
        "venice-client",
        "bash",
        "-c",
        "java -jar " + jar + " --empty-push --url " + veniceControllerUrl + " --cluster " + clusterName + " --store "
            + storeName + " --push-id init --store-size 1000");
  }

  private void updateVeniceStoreQuotas(String veniceControllerUrl, String jar, String clusterName, String storeName)
      throws Exception {
    execByServiceAsssertNoStdErr(
        "venice-client",
        "bash",
        "-c",
        "java -jar " + jar + " --update-store --url " + veniceControllerUrl + " --cluster " + clusterName + " --store "
            + storeName + " --storage-quota -1 --incremental-push-enabled true");

    execByServiceAsssertNoStdErr(
        "venice-client",
        "bash",
        "-c",
        "java -jar " + jar + " --update-store --url " + veniceControllerUrl + " --cluster " + clusterName + " --store "
            + storeName + " --read-quota 1000000");
  }

  private void createVeniceStore(
      String veniceControllerUrl,
      String keyFile,
      String valueFile,
      String jar,
      String clusterName,
      String storeName) throws Exception {
    execByServiceAsssertNoStdErr(
        "venice-client",
        "bash",
        "-c",
        "java -jar " + jar + " --new-store --url " + veniceControllerUrl + " --cluster " + clusterName + " --store "
            + storeName + " --key-schema-file " + keyFile + " --value-schema-file " + valueFile);
  }

  private void saveKeyValueSchemaFiles(String keyAsvc, String valueAsvc, String keyFile, String valueFile)
      throws Exception {
    execByServiceAsssertNoStdErr("venice-client", "bash", "-c", "echo '" + keyAsvc + "' > " + keyFile);
    execByServiceAsssertNoStdErr("venice-client", "bash", "-c", "echo '" + valueAsvc + "' > " + valueFile);
  }

  private void createTopics() throws Exception {
    execByServiceAsssertNoStdErr(
        "broker",
        "bash",
        "-c",
        "/pulsar/bin/pulsar-admin topics create-partitioned-topic -p 1 public/default/venice_admin_venice-cluster0");
    execByServiceAsssertNoStdErr(
        "broker",
        "bash",
        "-c",
        "/pulsar/bin/pulsar-admin topics create-partitioned-topic -p 1 t1/n1/input");
  }

  private ExecResult execByService(String serviceName, String... command) throws Exception {
    Optional<ContainerState> container = environment.getContainerByServiceName(serviceName);
    if (!container.isPresent()) {
      throw new Exception("Container not found: " + serviceName);
    }
    ExecResult res = container.get().execInContainer(command);

    String cmdStr = Arrays.toString(command);
    LOGGER
        .info("\t{} Exec\n\t{}\n\tStdOut:\n{}\n\tStdErr:\n{}\n", serviceName, cmdStr, res.getStdout(), res.getStderr());
    return res;
  }

  private ExecResult execByServiceAsssertNoStdErr(String serviceName, String... command) throws Exception {
    ExecResult res = execByService(serviceName, command);
    if (!res.getStderr().isEmpty()) {
      throw new Exception("StdErr is not empty: " + res.getStderr());
    }
    return res;
  }

  private void setRetention() throws Exception {
    execByServiceAsssertNoStdErr(
        "broker",
        "bash",
        "-c",
        "/pulsar/bin/pulsar-admin namespaces set-retention --size 10M --time 100 public/default");
    execByServiceAsssertNoStdErr(
        "broker",
        "bash",
        "-c",
        "/pulsar/bin/pulsar-admin namespaces set-retention --size 10M --time 100 t1/n1");
  }

  private void createNamespace() throws Exception {
    execByServiceAsssertNoStdErr("broker", "bash", "-c", "/pulsar/bin/pulsar-admin tenants create t1");
    execByServiceAsssertNoStdErr("broker", "bash", "-c", "/pulsar/bin/pulsar-admin namespaces create t1/n1");
  }

}
