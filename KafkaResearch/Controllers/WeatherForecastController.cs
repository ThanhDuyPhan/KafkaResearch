using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using System.Threading;

namespace KafkaResearch.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ControllerBase
    {
        private const string KAFKA_SERVER = "host1:9092";
        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        private readonly ILogger<WeatherForecastController> _logger;

        public WeatherForecastController(ILogger<WeatherForecastController> logger)
        {
            _logger = logger;
        }

        [HttpGet(Name = "GetWeatherForecast")]
        public IEnumerable<WeatherForecast> Get()
        {
            return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
                TemperatureC = Random.Shared.Next(-20, 55),
                Summary = Summaries[Random.Shared.Next(Summaries.Length)]
            })
            .ToArray();
        }

        [HttpPost("msg")]
        public async Task<OkResult> PushMessageAsync(string msg = "Hello Kafka")
        {
            var config = new ProducerConfig
            {
                BootstrapServers = KAFKA_SERVER,
            };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                var result = await producer.ProduceAsync("weblog", new Message<Null, string> { Value = msg });
            }
            return Ok();
        }

        [HttpGet("msg")]
        public async Task<OkResult> ListenMessageAsync()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = KAFKA_SERVER,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe("weblog");

                while (true)
                {
                    var consumeResult = consumer.Consume();
                    Console.WriteLine(consumeResult.Value);
                }

                consumer.Close();
            }
            return Ok();
        }
    }
}
