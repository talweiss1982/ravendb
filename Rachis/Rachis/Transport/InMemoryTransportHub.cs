using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Threading;

using Rachis.Interfaces;
using Rachis.Messages;

using Raven.Abstractions.Logging;

namespace Rachis.Transport
{
    public class InMemoryTransportHub
    {
        private readonly ConcurrentDictionary<string, BlockingCollection<MessageContext>> _messageQueue =
            new ConcurrentDictionary<string, BlockingCollection<MessageContext>>();

        private readonly HashSet<string> _disconnectedNodes = new HashSet<string>();

        private readonly HashSet<string> _disconnectedNodesFromSending = new HashSet<string>();

        private readonly Dictionary<string, InMemoryTransport> _transports = new Dictionary<string, InMemoryTransport>();

        public readonly ILog Log = LogManager.GetLogger(typeof(InMemoryTransportHub).FullName);

        public ConcurrentDictionary<string, BlockingCollection<MessageContext>> MessageQueue
        {
            get { return _messageQueue; }
        }

        public ITransport CreateTransportFor(string from)
        {
            InMemoryTransport value;
            if (_transports.TryGetValue(from, out value))
                return value;
            value = new InMemoryTransport(this, from);
            _transports[from] = value;
            return value;
        }

        public class InMemoryTransport : ITransport
        {
            private readonly InMemoryTransportHub _parent;
            private readonly string _from;

            public readonly ILog Log;

            public InMemoryTransport(InMemoryTransportHub parent, string from)
            {
                _parent = parent;
                _from = from;
                Log = LogManager.GetLogger(typeof (InMemoryTransport).FullName + "." + from);
            }

            public string From
            {
                get { return _from; }
            }

            public bool TryReceiveMessage(int timeout, CancellationToken cancellationToken, out MessageContext messageContext)
            {
                return _parent.TryReceiveMessage(_from, timeout, cancellationToken, out messageContext);
            }


            public void Stream(NodeConnectionInfo dest, InstallSnapshotRequest snapshotRequest, Action<Stream> streamWriter)
            {
                var stream = new MemoryStream();
                streamWriter(stream);
                stream.Position = 0;

                _parent.AddToQueue(this, dest.Name, snapshotRequest, stream);
            }

            public void Send(NodeConnectionInfo dest, CanInstallSnapshotRequest req)
            {
                _parent.AddToQueue(this, dest.Name, req);
            }

            public void SendInternal(string dest, string from, object msg)
            {
                InMemoryTransport srcTransport;
                if(!_parent._transports.TryGetValue(from, out srcTransport))
                    throw new ArgumentException("Invalid from node","from");
                _parent.AddToQueue(srcTransport, dest, msg);
            }

            public void Send(NodeConnectionInfo dest, TimeoutNowRequest req)
            {
                _parent.AddToQueue(this, dest.Name, req);
            }

            public void Send(NodeConnectionInfo dest, DisconnectedFromCluster req)
            {
                _parent.AddToQueue(this, dest.Name, req);
            }

            public void Send(NodeConnectionInfo dest, AppendEntriesRequest req)
            {
                _parent.AddToQueue(this, dest.Name, req);
            }

            public void Send(NodeConnectionInfo dest, RequestVoteRequest req)
            {
                _parent.AddToQueue(this, dest.Name, req);
            }

            public void SendToSelf(AppendEntriesResponse resp)
            {
                _parent.AddToQueue(this, From, resp);
            }

            public void ForceTimeout()
            {
                _parent.AddToQueue(this, From, new TimeoutException(), evenIfDisconnected: true);
            }

            public void Dispose()
            {
            }
        }

        private void AddToQueue<T>(InMemoryTransport src, string dest, T message, Stream stream = null,
            bool evenIfDisconnected = false)
        {			
            //if destination is considered disconnected --> drop the message so it never arrives
            if ((
                _disconnectedNodes.Contains(dest) ||
                _disconnectedNodesFromSending.Contains(src.From)
                ) && evenIfDisconnected == false)
                return;

            var newMessage = new InMemoryMessageContext(src)
            {
                Destination = dest,
                Message = message,
                Stream = stream
            };

            _messageQueue.AddOrUpdate(dest, new BlockingCollection<MessageContext> { newMessage },
                (destination, envelopes) =>
                {
                    envelopes.Add(newMessage);
                    return envelopes;
                });
        }

        private class InMemoryMessageContext : MessageContext
        {
            private readonly InMemoryTransport _parent;
            public string Destination { get; set; }

            public InMemoryMessageContext(InMemoryTransport parent)
            {
                _parent = parent;
            }

            public override void Reply(CanInstallSnapshotResponse resp)
            {
                _parent.SendInternal(_parent.From, Destination, resp);
            }

            public override void Reply(InstallSnapshotResponse resp)
            {
                _parent.SendInternal(_parent.From, Destination, resp);
            }

            public override void Reply(AppendEntriesResponse resp)
            {
                _parent.SendInternal(_parent.From, Destination, resp);
            }

            public override void Reply(RequestVoteResponse resp)
            {
                _parent.SendInternal(_parent.From, Destination, resp);
            }

            public override void ExecuteInEventLoop(Action action)
            {
                _parent.SendInternal(_parent.From, _parent.From, action);
            }

            public override void Done()
            {
                // nothing to do here.
            }

            public override void Error(Exception exception)
            {
                _parent.Log.Warn("Error processing message", exception);
            }
        }

        public void DisconnectNodeSending(string node)
        {
            _disconnectedNodesFromSending.Add(node);
        }

        public void ReconnectNodeSending(string node)
        {
            _disconnectedNodesFromSending.RemoveWhere(n => n.Equals(node, StringComparison.InvariantCultureIgnoreCase));
        }

        public void DisconnectNode(string node)
        {
            _disconnectedNodes.Add(node);
        }

        public void ReconnectNode(string node)
        {
            _disconnectedNodes.RemoveWhere(n => n.Equals(node, StringComparison.InvariantCultureIgnoreCase));
        }

        public bool TryReceiveMessage(string dest, int timeout, CancellationToken cancellationToken,
            out MessageContext messageContext)
        {
            if (timeout < 0)
                timeout = 0;
            messageContext = null;
            var messageQueue = _messageQueue.GetOrAdd(dest, s => new BlockingCollection<MessageContext>());
            var tryReceiveMessage = messageQueue.TryTake(out messageContext, timeout, cancellationToken);
            if (!tryReceiveMessage || _disconnectedNodes.Contains(dest) ||
                    messageContext.Message is TimeoutException) return false;
            if (Log.IsDebugEnabled)
                Log.Debug($"Failed to recive a message for {dest} within {timeout}ms");
            return true;
        }
    }
}
