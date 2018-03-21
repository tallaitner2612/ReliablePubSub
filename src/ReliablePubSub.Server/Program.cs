using System;
using System.Collections.Generic;
using System.Runtime.Remoting.Channels;
using System.Threading;
using System.Threading.Tasks;
using ReliablePubSub.Common;

namespace ReliablePubSub.Server
{
    class Program
    {
        private static void Main(string[] args)
        {
            //do
            //{
            //    Console.WriteLine("Running");
            //    using (var server = new ReliableServer(TimeSpan.FromSeconds(2), "tcp://*:6669"))
            //    {
            //        long id = 0;
            //        for (int i = 0; i < 100; i++)
            //        {
            //            NetMQMessage message = new NetMQMessage();
            //            message.Append("topic1");
            //            message.Append(DateTime.UtcNow.ToString());
            //            server.Publish(message);

            //            Thread.Sleep(100);
            //        }
            //    }
            //    Console.WriteLine("Stopped");
            //} while (Console.ReadKey().Key != ConsoleKey.Escape);

            var knownTypes = new Dictionary<Type, TypeConfig>();
            knownTypes.Add(typeof(MyMessage), new TypeConfig
            {
                Serializer = new WireSerializer(),
                Comparer = new DefaultComparer<MyMessage>(),
                KeyExtractor = new DefaultKeyExtractor<MyMessage>(x => x.Key)
            });

            var topics = new Dictionary<string, Type>();
            topics.Add("topic1", typeof(MyMessage));
            int counter = 0;
            using (var tokenSource = new CancellationTokenSource())
            using (var publisher = new Publisher("tcp://*", 6669, 6668, topics.Keys))
            {
                var tasks = new List<Task>();

                for (int i = 0; i < 100; i++)
                {
                    var t = Task.Run(() =>
                    {

                        long id = 0;
                        var rnd = new Random(1);
                        while (!tokenSource.IsCancellationRequested)
                        {
                            var message = new MyMessage
                            {
                                Id = id++,
                                Key = rnd.Next(1, 1000).ToString(),
                                Body = $"T:{Thread.CurrentThread.ManagedThreadId} Body: {Guid.NewGuid().ToString()}",
                                TimeStamp = DateTime.UtcNow
                            };
                            publisher.Publish(knownTypes, "topic1", message);
                            Console.Title = $"Sent: {Interlocked.Increment(ref counter)}";
                            Thread.Sleep(DateTime.UtcNow.Second % 30 == 0 ? 1000 : 100);
                        }
                    }, tokenSource.Token);
                    tasks.Add(t);
                }
                while (Console.ReadKey().Key != ConsoleKey.Escape) { }
                tokenSource.Cancel();
                Task.WaitAll(tasks.ToArray(), 5000);
            }
        }
    }
}