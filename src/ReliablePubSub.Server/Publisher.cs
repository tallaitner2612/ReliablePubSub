using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NetMQ;
using ReliablePubSub.Common;

namespace ReliablePubSub.Server
{
    class Publisher : IDisposable
    {

        private readonly ReliableServer _publishServer;
        private readonly SnapshotServer _snapshotServer;
        private readonly SnapshotCache _snapshotCache;
        private readonly BlockingCollection<NetMQMessage> _buffer = new BlockingCollection<NetMQMessage>();
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private readonly Task _consumerTask;

        public Publisher(string baseAddress, ushort publisherPort, ushort snapshotPort, IEnumerable<string> topics)
        {
            _snapshotCache = new SnapshotCache(topics);
            _publishServer = new ReliableServer(TimeSpan.FromSeconds(5), $"{baseAddress}:{publisherPort}");
            _snapshotServer = new SnapshotServer($"{baseAddress}:{snapshotPort}", _snapshotCache);
            _consumerTask = Task.Factory.StartNew(ConsumeMessages, _cancellationTokenSource,
                TaskCreationOptions.LongRunning);
        }

        private void ConsumeMessages(object state)
        {
            foreach (var message in _buffer.GetConsumingEnumerable(_cancellationTokenSource.Token))
                _publishServer.Publish(message);
        }

        public void Publish(string topic, string key, byte[] data)
        {
            _snapshotCache.AddOrUpdate(topic, key, data);
            var message = NetMqMessageExtensions.CreateMessage(topic, data);
            _buffer.Add(message);
        }

        public void Dispose()
        {
            _cancellationTokenSource.Cancel();
            _consumerTask?.Dispose();
            _publishServer?.Dispose();
            _snapshotServer?.Dispose();
        }
    }
}