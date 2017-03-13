﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Rachis
{
    /// <summary>
    /// This class implements the leader behavior. Note that only a single thread
    /// actually does work in here, the leader thread. All other work is requested 
    /// from it and it is done
    /// </summary>
    public class Leader : IDisposable
    {
        private Task _topologyModification;
        private readonly RachisConsensus _engine;

        private TaskCompletionSource<object> _newEntriesArrived = new TaskCompletionSource<object>();

        private readonly ConcurrentDictionary<long, CommandState> _entries =
            new ConcurrentDictionary<long, CommandState>();

        private class CommandState
        {
            public long CommandIndex;
            public TaskCompletionSource<long> TaskCompletionSource;
            public Action<TaskCompletionSource<long>> OnNotify;
        }

        private int _hasNewTopology;
        private readonly ManualResetEvent _newEntry = new ManualResetEvent(false);
        private readonly ManualResetEvent _voterResponded = new ManualResetEvent(false);
        private readonly ManualResetEvent _promotableUpdated = new ManualResetEvent(false);
        private readonly ManualResetEvent _shutdownRequested = new ManualResetEvent(false);
        private readonly ManualResetEvent _noop = new ManualResetEvent(false);
        private long _lowestIndexInEntireCluster;

        private readonly Dictionary<string, FollowerAmbassador> _voters =
            new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, FollowerAmbassador> _promotables =
            new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, FollowerAmbassador> _nonVoters =
            new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);

        private Thread _thread;

        public long LowestIndexInEntireCluster
        {
            get { return _lowestIndexInEntireCluster; }
            set { Interlocked.Exchange(ref _lowestIndexInEntireCluster, value); }
        }

        public Leader(RachisConsensus engine)
        {
            _engine = engine;
        }

        public bool Running
        {
            get { return Volatile.Read(ref _running); }
            private set { Volatile.Write(ref _running, value); }
        }

        public void Start()
        {
            Running = true;
            ClusterTopology clusterTopology;
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                clusterTopology = _engine.GetTopology(context);
            }

            RefreshAmbassadors(clusterTopology);

            _thread = new Thread(Run)
            {
                Name =
                    $"Consensus Leader - {_engine.Tag} in term {_engine.CurrentTerm}",
                IsBackground = true
            };
            _thread.Start();
        }

        public void StepDown()
        {
            if(_voters.Count == 0)
                throw new InvalidOperationException("Cannot step down when I'm the only voter int he cluster");
            var nextLeader = _voters.Values.OrderByDescending(x => x.FollowerMatchIndex).ThenByDescending(x=>x.LastReplyFromFollower).First();
            nextLeader.ForceElectionsNow = true;
            var old = Interlocked.Exchange(ref _newEntriesArrived, new TaskCompletionSource<object>());
            old.TrySetResult(null);
        }

        private void RefreshAmbassadors(ClusterTopology clusterTopology)
        {
            var old = new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);
            foreach (var peers in new[] { _voters, _promotables, _nonVoters })
            {
                foreach (var peer in peers)
                {
                    old[peer.Key] = peer.Value;
                }
                peers.Clear();
            }

            foreach (var voter in clusterTopology.Voters)
            {
                if (voter.Key == _engine.Tag)
                    continue; // we obviously won't be applying to ourselves

                FollowerAmbassador existingInstance;
                if (old.TryGetValue(voter.Key, out existingInstance))
                {
                    existingInstance.UpdateLeaderWake(_voterResponded);
                    _voters.Add(voter.Key, existingInstance);
                    old.Remove(voter.Key);
                    continue; // already here
                }

                var ambasaddor = new FollowerAmbassador(_engine, this, _voterResponded, voter.Key, voter.Value, clusterTopology.ApiKey);
                _voters.Add(voter.Key, ambasaddor);
                _engine.AppendStateDisposable(this, ambasaddor);
                ambasaddor.Start();
            }

            foreach (var promotable in clusterTopology.Promotables)
            {
                FollowerAmbassador existingInstance;
                if (old.TryGetValue(promotable.Key, out existingInstance))
                {
                    existingInstance.UpdateLeaderWake(_promotableUpdated);
                    _promotables.Add(promotable.Key, existingInstance);
                    old.Remove(promotable.Key);
                    continue; // already here
                }

                var ambasaddor = new FollowerAmbassador(_engine, this, _promotableUpdated, promotable.Key, promotable.Value, clusterTopology.ApiKey);
                _promotables.Add(promotable.Key, ambasaddor);
                _engine.AppendStateDisposable(this, ambasaddor);
                ambasaddor.Start();
            }

            foreach (var nonVoter in clusterTopology.NonVotingMembers)
            {
                FollowerAmbassador existingInstance;
                if (_nonVoters.TryGetValue(nonVoter.Key, out existingInstance))
                {
                    existingInstance.UpdateLeaderWake(_noop);

                    _nonVoters.Add(nonVoter.Key, existingInstance);
                    old.Remove(nonVoter.Key);
                    continue; // already here
                }
                var ambasaddor = new FollowerAmbassador(_engine, this, _noop, nonVoter.Key, nonVoter.Value, clusterTopology.ApiKey);
                _nonVoters.Add(nonVoter.Key, ambasaddor);
                _engine.AppendStateDisposable(this, ambasaddor);
                ambasaddor.Start();
            }

            Task.Run(() =>
            {
                foreach (var ambasaddor in old)
                {
                    // it is not used by anything else, so we can close it
                    ambasaddor.Value.Dispose();
                }
            });
        }

        /// <summary>
        /// This is expected to run for a long time, and it cannot leak exceptions
        /// </summary>
        private void Run()
        {
            try
            {
                var handles = new WaitHandle[]
                {
                    _newEntry,
                    _voterResponded,
                    _promotableUpdated,
                    _shutdownRequested
                };

                var noopCmd = new DynamicJsonValue
                {
                    ["Command"] = "noop"
                };
                TransactionOperationContext context;
                using (_engine.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                using (var cmd = context.ReadObject(noopCmd, "noop-cmd"))
                {
                    _engine.InsertToLeaderLog(context, cmd, RachisEntryFlags.Noop);
                    tx.Commit();
                }
                _newEntry.Set(); //This is so the noop would register right away
                while (true)
                {
                    switch (WaitHandle.WaitAny(handles, _engine.ElectionTimeoutMs))
                    {
                        case 0: // new entry
                            _newEntry.Reset();
                            // release any waiting ambassadors to send immediately
                            var old = Interlocked.Exchange(ref _newEntriesArrived, new TaskCompletionSource<object>());
                            ThreadPool.QueueUserWorkItem(o => ((TaskCompletionSource<object>)o).TrySetResult(null), old);
                            if (_voters.Count == 0)
                                goto case 1;
                            break;
                        case 1: // voter responded
                            _voterResponded.Reset();
                            OnVoterConfirmation();
                            break;
                        case 2: // promotable updated
                            _promotableUpdated.Reset();
                            CheckPromotables();

                            break;
                        case WaitHandle.WaitTimeout:
                            break;
                        case 3: // shutdown requested
                            return;
                    }
                    EnsureThatWeHaveLeadership(VotersMajority);

                    var lowestIndexInEntireCluster = GetLowestIndexInEntireCluster();
                    if (lowestIndexInEntireCluster != LowestIndexInEntireCluster)
                    {
                        using (_engine.ContextPool.AllocateOperationContext(out context))
                        using (context.OpenWriteTransaction())
                        {
                            _engine.TruncateLogBefore(context, lowestIndexInEntireCluster);
                            LowestIndexInEntireCluster = lowestIndexInEntireCluster;
                            context.Transaction.Commit();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info("Error when running leader behavior", e);
                }
                try
                {
                    _engine.SwitchToCandidateState("An error occured during leadership - " + e);
                }
                catch (Exception e2)
                {
                    if (_engine.Log.IsOperationsEnabled)
                    {
                        _engine.Log.Operations("After leadership failure, could not setup switch to candidate state", e2);
                    }
                }
            }
        }

        private void VoteOfNoConfidence()
        {
            if (TimeoutEvent.Disable)
                return;

            var sb = new StringBuilder();
            var now = DateTime.UtcNow;
            foreach (var ambassador in _voters)
            {
                var followerAmbassador = ambassador.Value;
                var sinceLastReply = (long)(now - followerAmbassador.LastReplyFromFollower).TotalMilliseconds;
                var sinceLastSend = (long) (now - followerAmbassador.LastSendToFollower).TotalMilliseconds;
                var lastMsg = followerAmbassador.LastSendMsg;
                sb.AppendLine(
                    $"{followerAmbassador.Tag}: Got last reply {sinceLastReply:#,#;;0} ms ago and sent {sinceLastSend:#,#;;0} ms ({lastMsg}) - {followerAmbassador.Status}");
            }

            throw new TimeoutException(
                "Too long has passed since we got a confirmation from the majority of the cluster that this node is still the leader." +
                Environment.NewLine +
                "Assuming that I'm not the leader and stepping down." + 
                Environment.NewLine +
                sb
                );
        }

        private long _lastCommit;
        private void OnVoterConfirmation()
        {
            TransactionOperationContext context;
            if (Interlocked.CompareExchange(ref _hasNewTopology, 0, 1) != 0)
            {
                ClusterTopology clusterTopology;
                using (_engine.ContextPool.AllocateOperationContext(out context))
                using (context.OpenReadTransaction())
                {
                    clusterTopology = _engine.GetTopology(context);
                }
                RefreshAmbassadors(clusterTopology);
            }

            var maxIndexOnQuorum = GetMaxIndexOnQuorum(VotersMajority);

            if (_lastCommit >= maxIndexOnQuorum ||
                maxIndexOnQuorum == 0)
                return; // nothing to do here

            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenWriteTransaction())
            {
                _lastCommit = _engine.GetLastCommitIndex(context);

                if (_lastCommit >= maxIndexOnQuorum)
                    return; // nothing to do here

                if (_engine.GetTermForKnownExisting(context, maxIndexOnQuorum) < _engine.CurrentTerm)
                    return;// can't commit until at least one entry from our term has been published

                _engine.TakeOffice();

                _lastCommit = maxIndexOnQuorum;

                _engine.Apply(context, maxIndexOnQuorum, this);

                _lastCommit = maxIndexOnQuorum;

                context.Transaction.Commit();
            }

            foreach (var kvp in _entries)
            {
                if (kvp.Key > _lastCommit)
                    continue;

                CommandState value;
                if (_entries.TryRemove(kvp.Key, out value))
                {
                    ThreadPool.QueueUserWorkItem(o =>
                    {
                        var tuple = (CommandState)o;
                        if (tuple.OnNotify != null)
                        {
                            tuple.OnNotify(tuple.TaskCompletionSource);
                            return;
                        }
                        tuple.TaskCompletionSource.TrySetResult(tuple.CommandIndex);
                    }, value);
                }
            }
            if (_entries.Count != 0)
            {
                // we have still items to process, run them in 1 node cluster
                // and speed up the followers ambassadors if they can
                _newEntry.Set();
            }
        }

        private void EnsureThatWeHaveLeadership(int majority)
        {
            var now = DateTime.UtcNow;
            var peersHeardFromInElectionTimeout = 1; // we count as a node :-)
            foreach (var voter in _voters.Values)
            {
                if ((now - voter.LastReplyFromFollower).TotalMilliseconds < _engine.ElectionTimeoutMs)
                    peersHeardFromInElectionTimeout++;
            }
            if (peersHeardFromInElectionTimeout < majority)
                VoteOfNoConfidence(); // we didn't get enough votes to still remain the leader
        }

        /// <summary>
        /// This method works on the match indexes, assume that we have three nodes
        /// A, B and C, and they have the following index values:
        /// 
        /// { A = 4, B = 3, C = 2 }
        /// 
        /// 
        /// In this case, the quorum agrees on 3 as the committed index.
        /// 
        /// Why? Because A has 4 (which implies that it has 3) and B has 3 as well.
        /// So we have 2 nodes that have 3, so that is the quorom.
        /// </summary>
        private readonly SortedList<long, int> _nodesPerIndex = new SortedList<long, int>();

        private bool _running;
        private int VotersMajority => (_voters.Count + 1) / 2 + 1;

        protected long GetLowestIndexInEntireCluster()
        {
            long lowestIndex;
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                lowestIndex = _engine.GetLastEntryIndex(context);
            }

            foreach (var voter in _voters.Values)
            {
                lowestIndex = Math.Min(lowestIndex, voter.FollowerMatchIndex);
            }

            foreach (var promotable in _promotables.Values)
            {
                lowestIndex = Math.Min(lowestIndex, promotable.FollowerMatchIndex);
            }

            foreach (var nonVoter in _nonVoters.Values)
            {
                lowestIndex = Math.Min(lowestIndex, nonVoter.FollowerMatchIndex);
            }

            return lowestIndex;
        }

        protected long GetMaxIndexOnQuorum(int minSize)
        {
            _nodesPerIndex.Clear();
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                _nodesPerIndex[_engine.GetLastEntryIndex(context)] = 1;
            }

            foreach (var voter in _voters.Values)
            {
                int count;
                var voterIndex = voter.FollowerMatchIndex;
                _nodesPerIndex.TryGetValue(voterIndex, out count);
                _nodesPerIndex[voterIndex] = count + 1;
            }
            var votesSoFar = 0;

            for (int i = _nodesPerIndex.Count - 1; i >= 0; i--)
            {
                votesSoFar += _nodesPerIndex.Values[i];
                if (votesSoFar >= minSize)
                    return _nodesPerIndex.Keys[i];
            }
            return 0;
        }

        private void CheckPromotables()
        {
            long lastIndex;
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                lastIndex = _engine.GetLastEntryIndex(context);
            }

            foreach (var ambasaddor in _promotables)
            {
                if (ambasaddor.Value.FollowerMatchIndex != lastIndex)
                    continue;

                Task task;
                TryModifyTopology(ambasaddor.Key, ambasaddor.Value.Url, TopologyModification.Voter, out task);

                _promotableUpdated.Set();
                break;
            }

        }

        public Task<long> PutAsync(BlittableJsonReaderObject cmd)
        {
            long index;

            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenWriteTransaction())
            {
                index = _engine.InsertToLeaderLog(context, cmd, RachisEntryFlags.StateMachineCommand);
                context.Transaction.Commit();
            }
            var tcs = new TaskCompletionSource<long>();
            _entries[index] = new CommandState
            {
                CommandIndex = index,
                TaskCompletionSource = tcs
            };

            _newEntry.Set();
            return tcs.Task;
        }

        public void Dispose()
        {
            Running = false;
            _shutdownRequested.Set();
            ThreadPool.QueueUserWorkItem(_ =>
            {
                _newEntriesArrived.TrySetCanceled();
                var lastStateChangeReason = _engine.LastStateChangeReason;
                TimeoutException te = null;
                if (string.IsNullOrEmpty(lastStateChangeReason) == false)
                    te = new TimeoutException(lastStateChangeReason);
                foreach (var entry in _entries)
                {
                    if (te == null)
                    {
                        entry.Value.TaskCompletionSource.TrySetCanceled();
                    }
                    else
                    {
                        entry.Value.TaskCompletionSource.TrySetException(te);
                    }
                }
            });

            if (_thread != null && _thread.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                _thread.Join();

            var ae = new ExceptionAggregator("Could not properly dispose Leader");
            foreach (var ambasaddor in _nonVoters)
            {
                ae.Execute(ambasaddor.Value.Dispose);
            }

            foreach (var ambasaddor in _promotables)
            {
                ae.Execute(ambasaddor.Value.Dispose);
            }
            foreach (var ambasaddor in _voters)
            {
                ae.Execute(ambasaddor.Value.Dispose);
            }
        

            _newEntry.Dispose();
            _voterResponded.Dispose();
            _promotableUpdated.Dispose();
            _shutdownRequested.Dispose();
            _noop.Dispose();
        }

        public Task WaitForNewEntries()
        {
            return _newEntriesArrived.Task;
        }

        public enum TopologyModification
        {
            Voter,
            Promotable,
            NonVoter,
            Remove
        }

        public bool TryModifyTopology(string nodeTag, string nodeUrl, TopologyModification modification, out Task task, bool validateNotInTopology = false)
        {
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenWriteTransaction())
            {
                var existing = Interlocked.CompareExchange(ref _topologyModification, null, null);
                if (existing != null)
                {
                    task = existing;
                    return false;
                }

                var clusterTopology = _engine.GetTopology(context);

                if (nodeTag == null)
                {
                    nodeTag = GenerateNodeTag(clusterTopology);
                }


                if (validateNotInTopology && clusterTopology.Contains(nodeTag))
                {
                    throw new InvalidOperationException($"Was requested to modify the topology for node={nodeTag} " +
                                                        $"with validation that it is not contained by the topology but current topology contains it.");
                }

                var newVotes = new Dictionary<string, string>(clusterTopology.Voters);
                newVotes.Remove(nodeTag);
                var newPromotables = new Dictionary<string, string>(clusterTopology.Promotables);
                newPromotables.Remove(nodeTag);
                var newNonVotes = new Dictionary<string, string>(clusterTopology.NonVotingMembers);
                newNonVotes.Remove(nodeTag);

                var highestNodeId = newVotes.Keys.Concat(newPromotables.Keys).Concat(newNonVotes.Keys).Concat(new [] {nodeTag}).Max();

                switch (modification)
                {
                    case TopologyModification.Voter:
                        Debug.Assert(nodeUrl != null);
                        newVotes[nodeTag] = nodeUrl;
                        break;
                    case TopologyModification.Promotable:
                        Debug.Assert(nodeUrl != null);
                        newPromotables[nodeTag] = nodeUrl;
                        break;
                    case TopologyModification.NonVoter:
                        Debug.Assert(nodeUrl != null);
                        newNonVotes[nodeTag] = nodeUrl;
                        break;
                    case TopologyModification.Remove:
                        if (clusterTopology.Contains(nodeTag) == false)
                        {
                            throw new InvalidOperationException($"Was requested to remove node={nodeTag} from the topology " +
                                                        $"but it is not contained by the topology.");
                        }
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(modification), modification, null);
                }

                clusterTopology = new ClusterTopology(clusterTopology.TopologyId, clusterTopology.ApiKey,
                      newVotes,
                      newPromotables,
                      newPromotables,
                      highestNodeId
                  );

                var topologyJson = _engine.SetTopology(context, clusterTopology);

                var index = _engine.InsertToLeaderLog(context, topologyJson, RachisEntryFlags.Topology);
                var tcs = new TaskCompletionSource<long>();
                _entries[index] =new CommandState
                {
                    TaskCompletionSource = tcs,
                    CommandIndex = index
                };
                _topologyModification = task = tcs.Task.ContinueWith(_ =>
                {
                    Interlocked.Exchange(ref _topologyModification, null);
                });
                context.Transaction.Commit();
            }
            Interlocked.Exchange(ref _hasNewTopology, 1);
            _voterResponded.Set();
            _newEntry.Set();

            return true;
        }

        private static string GenerateNodeTag(ClusterTopology clusterTopology)
        {
            if (clusterTopology.LastNodeId.Length == 0)
            {
                return "A";
            }

            if (clusterTopology.LastNodeId[clusterTopology.LastNodeId.Length - 1] + 1 > 'Z')
            {
                return clusterTopology.LastNodeId + "A";
            }

            var lastChar = (char)(clusterTopology.LastNodeId[clusterTopology.LastNodeId.Length - 1] + 1);
            return clusterTopology.LastNodeId.Substring(0, clusterTopology.LastNodeId.Length - 1) + lastChar;
        }

        public void SetStateOf(long index, Action<TaskCompletionSource<long>> onNotify)
        {
            CommandState value;
            if (_entries.TryGetValue(index, out value))
            {
                value.OnNotify = onNotify;
            }
        }
    }
}