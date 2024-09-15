using System;
using System.Text.Json;
using Confluent.Kafka;

public class Program
{
    public static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "18.218.28.236:29092",
            GroupId = "consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Null, string>(config).Build();
        consumer.Subscribe("books");

        Console.WriteLine("Consumer started. Listening for messages...");

        while (true)
        {
            var cr = consumer.Consume();
            var book = JsonSerializer.Deserialize<Book>(cr.Message.Value);
            if (book != null)
            {
                Console.WriteLine($"Consumed: {book.Title} by {book.Author} ({book.Year})");
            }
        }
    }
}

public class Book
{
    public string Title { get; set; }
    public string Author { get; set; }
    public int Year { get; set; }
}
