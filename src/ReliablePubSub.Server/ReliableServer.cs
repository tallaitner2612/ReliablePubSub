using System;
using NetMQ;
using NetMQ.Sockets;

namespace ReliablePubSub.Server
{
    class ReliableServer : IDisposable
    {
        private readonly TimeSpan _heartbeatInterval;
        private const string PublishMessageCommand = "P";
        private const string ReceiveTimeoutCommand = "RT";
        private const string WelcomeMessage = "WM";
        private const string HeartbeatMessage = "HB";

        private readonly string _address;
        private readonly NetMQActor _actor;
        private XPublisherSocket _publisherSocket;
        private NetMQTimer _heartbeatTimer;
        private NetMQTimer _receiveTimeoutTimer;
        private NetMQPoller _poller;
        private PairSocket _shim;
        private readonly int _receiveTimeout;
        private readonly Action _onReceiveTimeout;

        public ReliableServer(TimeSpan heartbeatInterval, string address, int receiveTimeout = 0, Action onReceiveTimeout = null)
        {
            _address = address;
            _receiveTimeout = receiveTimeout;
            _onReceiveTimeout = onReceiveTimeout;
            _heartbeatInterval = heartbeatInterval;

            // actor is like thread with builtin pair sockets connect the user thread with the actor thread
            _actor = NetMQActor.Create(Run);
        }

        public void Dispose()
        {
            _actor.Dispose();
        }

        private void Run(PairSocket shim)
        {
            _shim = shim;
            using (_publisherSocket = new XPublisherSocket())
            {
                _publisherSocket.SetWelcomeMessage(WelcomeMessage);
                _publisherSocket.Bind(_address);

                _publisherSocket.ReceiveReady += DropPublisherSubscriptions;

                _heartbeatTimer = new NetMQTimer(_heartbeatInterval);
                _heartbeatTimer.Elapsed += OnHeartbeatTimerElapsed;

                if (_receiveTimeout > 0)
                {
                    _receiveTimeoutTimer = new NetMQTimer(1000);
                    _receiveTimeoutTimer.Enable = false;
                    _receiveTimeoutTimer.Elapsed += OnReceiveTimeout; ;
                }

                shim.ReceiveReady += OnShimMessage;

                // signal the actor that the shim is ready to work
                shim.SignalOK();

                _poller = new NetMQPoller { _publisherSocket, shim, _heartbeatTimer, _receiveTimeoutTimer };
                // Polling until poller is cancelled
                _poller.Run();
            }
        }

        private void OnReceiveTimeout(object sender, NetMQTimerEventArgs e)
        {
            _actor.SendFrame(ReceiveTimeoutCommand);
        }

        private void OnHeartbeatTimerElapsed(object sender, NetMQTimerEventArgs e)
        {
            // Heartbeat timer elapsed, let's send another heartbeat
            _publisherSocket.SendFrame(HeartbeatMessage);
        }

        private void OnShimMessage(object sender, NetMQSocketEventArgs e)
        {
            string command = e.Socket.ReceiveFrameString();

            if (command == PublishMessageCommand)
            {
                // just forward the message to the publisher
                NetMQMessage message = e.Socket.ReceiveMultipartMessage();
                _publisherSocket.SendMultipartMessage(message);
                _receiveTimeoutTimer?.EnableAndReset();
            }
            else if (command == ReceiveTimeoutCommand)
            {
                _receiveTimeoutTimer.Enable = false;
                _onReceiveTimeout?.Invoke();
            }
            else if (command == NetMQActor.EndShimMessage)
            {
                // we got dispose command, we just stop the poller
                _poller.Stop();
            }
        }

        private void DropPublisherSubscriptions(object sender, NetMQSocketEventArgs e)
        {
            // just drop the subscription messages, we have to do that to Welcome message to work
            _publisherSocket.SkipMultipartMessage();
        }


        public void Publish(NetMQMessage message)
        {
            // we can use actor like NetMQSocket
            _actor.SendMoreFrame(PublishMessageCommand).SendMultipartMessage(message);
        }
    }
}
