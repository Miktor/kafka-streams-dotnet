using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Protobuf;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace sample_stream_registry
{
    public class Program
    {
        public static void BuildTopology(StreamBuilder builder)
        {
            builder.Table("test_input_table",
                    new SchemaProtobufSerDes<test_key>(),
                    new SchemaProtobufSerDes<test_message>(),
                    InMemory<test_key, test_message>.As("test_topic-store"))
                .ToStream()
                .To("test_output_topic");
        }

        static async Task Main(string[] args)
        {
            CancellationTokenSource source = new CancellationTokenSource();

            var config = new StreamConfig();
            config.ApplicationId = "test-app";
            config.BootstrapServers = "localhost:9092";
            config.SchemaRegistryUrl = "http://localhost:8081";
            config.AutoRegisterSchemas = true;

            StreamBuilder builder = new StreamBuilder();
            BuildTopology(builder);

            Topology t = builder.Build();

            KafkaStream stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) => { stream.Dispose(); };

            await stream.StartAsync();
        }
    }
}