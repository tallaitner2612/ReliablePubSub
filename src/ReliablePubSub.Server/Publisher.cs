using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
        private readonly BlockingCollection<Tuple<string, byte[]>> _buffer = new BlockingCollection<Tuple<string, byte[]>>();
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private readonly Task _consumerTask;

        private readonly int _publishInterval;
        private readonly int _maxBulkSize;
        private const string OnReceiveTimeoutTopic = "RT";
        private Dictionary<string, long> _messageIds;

        public Publisher(string baseAddress, ushort publisherPort, ushort snapshotPort, IEnumerable<string> topics, int publishInterval = 20, int maxBulkSize = 100)
        {
            _publishInterval = publishInterval;
            _maxBulkSize = maxBulkSize;
            _messageIds = topics.ToDictionary(x => x, x => 0L);
            _snapshotCache = new SnapshotCache(topics);
            _snapshotServer = new SnapshotServer($"{baseAddress}:{snapshotPort}", _snapshotCache);
            _publishServer = new ReliableServer(TimeSpan.FromSeconds(5), $"{baseAddress}:{publisherPort}", _publishInterval, OnReceiveTimeout);
            _consumerTask = Task.Factory.StartNew(ConsumeMessages, _cancellationTokenSource.Token,
                TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }


        private void ConsumeMessages()
        {
            var messageBulk = new List<byte[]>(_maxBulkSize);
            var lastPublish = DateTime.MinValue;
            var lastTopic = "";
            foreach (var tuple in _buffer.GetConsumingEnumerable(_cancellationTokenSource.Token))
            {
                if (tuple.Item2 != null)
                    messageBulk.Add(tuple.Item2);

                var currentTopic = tuple.Item1;
                if (string.IsNullOrEmpty(lastTopic) && currentTopic != OnReceiveTimeoutTopic)
                    lastTopic = currentTopic;

                var publishTopic = lastTopic;

                if ((DateTime.UtcNow.Subtract(lastPublish).TotalMilliseconds > _publishInterval || //publish bulk on elapsed interval
                         messageBulk.Count == _maxBulkSize || //publish bulk on max size
                         currentTopic != lastTopic) //publish bulk if topic changed (cannot send different topics in same bulk + should cover also case of recieve timeout)
                         && messageBulk.Count > 0)
                {
                    if (currentTopic != OnReceiveTimeoutTopic) //don't update topic if its receive timeout
                        lastTopic = currentTopic;

                    _publishServer.Publish(CreateMultipartMessage(publishTopic, messageBulk));
                    lastPublish = DateTime.UtcNow;
                    messageBulk = new List<byte[]>(_maxBulkSize);
                }
            }
        }

        private NetMQMessage CreateMultipartMessage(string publishTopic, List<byte[]> messageBulk)
        {
            var msgid = _messageIds[publishTopic]++;
            if (msgid < 0)
                _messageIds[publishTopic] = msgid = 0;

            var message = new NetMQMessage(messageBulk);
            message.Push(msgid);
            message.Push(publishTopic);
            return message;
        }

        private void OnReceiveTimeout()
        {
            Debug.WriteLine("OnReceiveTimeout");
            _buffer.Add(new Tuple<string, byte[]>(OnReceiveTimeoutTopic, null));
        }

        public void Publish(string topic, string key, byte[] data)
        {
            _snapshotCache.AddOrUpdate(topic, key, data);
            _buffer.Add(new Tuple<string, byte[]>(topic, data));
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