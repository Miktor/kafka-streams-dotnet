using Confluent.Type;
using sample_stream_registry;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Protobuf;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Decimal = Confluent.Type.Decimal;

namespace Streams.UnitTests;

public class Tests
{
    private TopologyTestDriver driver;

    [SetUp]
    public void Setup()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>();
        config.ApplicationId = "test-test-driver-app";
        config.SchemaRegistryUrl = "mock://test";
        config.AutoRegisterSchemas = true;

        StreamBuilder builder = new StreamBuilder();

        Program.BuildTopology(builder);

        Topology t = builder.Build();
        driver = new TopologyTestDriver(t, config);
    }

    [Test]
    public void Test()
    {
        var inputTopic = driver.CreateInputTopic<test_key, test_message>("test_input_table",
            new SchemaProtobufSerDes<test_key>(),
            new SchemaProtobufSerDes<test_message>());


        var outputTopic = driver.CreateOuputTopic<test_key, test_message>("test_output_topic", TimeSpan.FromSeconds(1),
            new SchemaProtobufSerDes<test_key>(),
            new SchemaProtobufSerDes<test_message>());

        inputTopic.PipeInput(new test_key() { VALUE = 1 }, new test_message()
        {
            INTVALUE = 1
        });

        Assert.NotNull(outputTopic.ReadValue());
    }
}