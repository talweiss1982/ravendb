﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Rachis
{
    public class RachisConsensus<TStateMachine> : RachisConsensus
        where TStateMachine : RachisStateMachine, new()
    {
        public RachisConsensus(StorageEnvironmentOptions options, string url, int? seed = null) : base(options, url, seed)
        {
        }

        public TStateMachine StateMachine;

        protected override void InitializeState(TransactionOperationContext context)
        {
            StateMachine = new TStateMachine();
            StateMachine.Initialize(this, context);
        }

        public override void Dispose()
        {
            SetNewState(State.Follower, new NullDisposable(), -1, "Disposing Rachis");
            StateMachine?.Dispose();
            base.Dispose();
        }

        public override void Apply(TransactionOperationContext context, long uptoInclusive)
        {
            StateMachine.Apply(context, uptoInclusive);
        }

        public override bool ShouldSnapshot(Slice slice, RootObjectType type)
        {
            return StateMachine.ShouldSnapshot(slice, type);
        }

        public override void SnapshotInstalled(TransactionOperationContext context)
        {
            StateMachine.OnSnapshotInstalled(context);
        }

        private class NullDisposable : IDisposable
        {
            public void Dispose()
            {

            }
        }
    }

    public abstract class RachisConsensus : IDisposable
    {
        public enum State
        {
            Passive,
            Candidate,
            Follower,
            LeaderElect,
            Leader
        }

        public State CurrentState { get; private set; }
        public TimeoutEvent Timeout { get; private set; }
        public string LastStateChangeReason => _lastStateChangeReason;

        private readonly StorageEnvironmentOptions _options;
        private readonly string _url;
        public TransactionContextPool ContextPool { get; private set; }
        private StorageEnvironment _persistentState;
        internal readonly Logger Log;

        private readonly List<IDisposable> _disposables = new List<IDisposable>();


        public long CurrentTerm { get; private set; }
        public string Url => _url;
        public string TempPath => _options.TempPath;

        private static readonly Slice GlobalStateSlice;
        private static readonly Slice CurrentTermSlice;
        private static readonly Slice VotedForSlice;
        private static readonly Slice LastCommitSlice;
        private static readonly Slice LastTruncatedSlice;
        private static readonly Slice TopologySlice;


        internal static readonly Slice EntriesSlice;
        internal static readonly TableSchema LogsTable;

        static RachisConsensus()
        {
            Slice.From(StorageEnvironment.LabelsContext, "GlobalState", out GlobalStateSlice);

            Slice.From(StorageEnvironment.LabelsContext, "CurrentTerm", out CurrentTermSlice);
            Slice.From(StorageEnvironment.LabelsContext, "VotedFor", out VotedForSlice);
            Slice.From(StorageEnvironment.LabelsContext, "LastCommit", out LastCommitSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Topology", out TopologySlice);
            Slice.From(StorageEnvironment.LabelsContext, "LastTruncated", out LastTruncatedSlice);


            Slice.From(StorageEnvironment.LabelsContext, "Entries", out EntriesSlice);

            /*
             
            index - int64 big endian
            term  - int64 little endian
            entry - blittable value
            flags - cmd, no op, topology, etc

             */
            LogsTable = new TableSchema();
            LogsTable.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
            });
        }

        public int ElectionTimeoutMs = Debugger.IsAttached ? 3000 : 300;

        private Leader _currentLeader;
        private TaskCompletionSource<object> _topologyChanged = new TaskCompletionSource<object>();
        private TaskCompletionSource<object> _stateChanged = new TaskCompletionSource<object>();
        private TaskCompletionSource<object> _commitIndexChanged = new TaskCompletionSource<object>();
        private int? _seed;
        private string _lastStateChangeReason;

        protected RachisConsensus(StorageEnvironmentOptions options, string url, int? seed = null)
        {
            _seed = seed;
            _options = options;
            _url = url;
            Log = LoggingSource.Instance.GetLogger<RachisConsensus>(options.BasePath);
        }

        public unsafe void Initialize()
        {
            try
            {
                _persistentState = new StorageEnvironment(_options);
                ContextPool = new TransactionContextPool(_persistentState);

                ClusterTopology topology;
                TransactionOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                {
                    LogsTable.Create(tx.InnerTransaction, EntriesSlice, 16);

                    var state = tx.InnerTransaction.CreateTree(GlobalStateSlice);
                    var read = state.Read(CurrentTermSlice);
                    if (read == null || read.Reader.Length != sizeof(long))
                    {
                        byte* ptr;
                        using (state.DirectAdd(CurrentTermSlice, sizeof(long), out ptr))
                            *(long*) ptr = CurrentTerm = 0;
                    }
                    else
                        CurrentTerm = read.Reader.ReadLittleEndianInt64();

                    topology = GetTopology(context);

                    InitializeState(context);

                    tx.Commit();
                }
                //We want to be able to reproduce rare issues that are related to timing
                var rand = _seed.HasValue ? new Random(_seed.Value) : new Random();
                Timeout = new TimeoutEvent(rand.Next(ElectionTimeoutMs / 3 * 2, ElectionTimeoutMs));

                // if we don't have a topology id, then we are passive
                // an admin needs to let us know that it is fine, either
                // by explicit bootstraping or by connecting us to a cluster
                if (topology.TopologyId == null ||
                    topology.Voters.Contains(_url) == false)
                {
                    CurrentState = State.Passive;
                    return;
                }

                CurrentState = State.Follower;
                if (topology.Voters.Length == 1)
                {
                    var electionTerm = CurrentTerm + 1;
                    using (ContextPool.AllocateOperationContext(out context))
                    using (var tx = context.OpenWriteTransaction())
                    {

                        CastVoteInTerm(context, electionTerm, Url);

                        tx.Commit();
                    }
                    SwitchToLeaderState(electionTerm, "I'm the only one in the cluster, so I'm the leader");
                }
                else
                    Timeout.Start(SwitchToCandidateStateOnTimeout);
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        protected abstract void InitializeState(TransactionOperationContext context);


        public async Task WaitForState(State state)
        {
            while (true)
            {
                var task = _stateChanged.Task;
                if (CurrentState == state)
                    return;
                await task;
            }
        }

        public async Task WaitForTopology(Leader.TopologyModification modification,string nodeUrl = null)
        {
            var url = nodeUrl ?? _url;
            while (true)
            {
                var task = _topologyChanged.Task;
                TransactionOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                using (context.OpenReadTransaction())
                {
                    var clusterTopology = GetTopology(context);
                    switch (modification)
                    {
                        case Leader.TopologyModification.Voter:
                            if (clusterTopology.Voters.Contains(url))
                                return;
                            break;
                        case Leader.TopologyModification.Promotable:
                            if (clusterTopology.Promotables.Contains(url))
                                return;
                            break;
                        case Leader.TopologyModification.NonVoter:
                            if (clusterTopology.NonVotingMembers.Contains(url))
                                return;
                            break;
                        case Leader.TopologyModification.Remove:
                            if (clusterTopology.Voters.Contains(url) == false &&
                                clusterTopology.Promotables.Contains(url) == false &&
                                clusterTopology.NonVotingMembers.Contains(url) == false)
                                return;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(modification), modification, null);
                    }
                }

                await task;
            }
        }

        public enum CommitIndexModification
        {
            Equal,
            GreaterOrEqual,
            AnyChange
        }

        public void SetNewState(State state, IDisposable disposable, long expectedTerm, string stateChangedReason)
        {
            List<IDisposable> toDispose;

            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenWriteTransaction()) // we use the write transaction lock here
            {
                if (expectedTerm != CurrentTerm && expectedTerm != -1)
                    throw new ConcurrencyException(
                        $"Attempted to switch state to {state} on expected term {expectedTerm} but the real term is {CurrentTerm}");

                _currentLeader = null;
                _lastStateChangeReason = stateChangedReason;
                toDispose = new List<IDisposable>(_disposables);

                _disposables.Clear();

                if (disposable != null)
                    _disposables.Add(disposable);
                else if(state != State.Passive)// if we are back to null state, wait to become candidate if no one talks to us
                    Timeout.Start(SwitchToCandidateStateOnTimeout);

                if (state == State.Passive)
                {
                    DeleteTopology(context);
                }
                context.Transaction.Commit();
            }

            UpdateState(state);

            Task.Run(() =>
            {
                foreach (var t in toDispose)
                {
                    t.Dispose();
                }
            });
        }

        public void TakeOffice()
        {
            if (CurrentState != State.LeaderElect)
                return;

            UpdateState(State.Leader);
        }


        private void UpdateState(State state)
        {
            CurrentState = state;
            ThreadPool.QueueUserWorkItem(
                _ => { Interlocked.Exchange(ref _stateChanged, new TaskCompletionSource<object>()).TrySetResult(null); });
        }

        public void AppendStateDisposable(IDisposable parentState, IDisposable disposeOnStateChange)
        {
            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenWriteTransaction()) // using write tx just for the lock here
            {
                if (_disposables.Count > 0 && ReferenceEquals(_disposables[0], parentState) == false)
                    throw new ConcurrencyException(
                        "Could not set the disposeOnStateChange because by the time we did it the parent state has changed");
                _disposables.Add(disposeOnStateChange);
            }
        }

        public void SwitchToLeaderState(long electionTerm, string reason)
        {
            if (Log.IsInfoEnabled)
            {
                Log.Info("Switching to leader state");
            }
            var leader = new Leader(this);
            SetNewState(State.LeaderElect, leader, electionTerm, reason);
            _currentLeader = leader;
            leader.Start();
        }

        public Task<long> PutAsync(BlittableJsonReaderObject cmd)
        {
            var leader = _currentLeader;
            if (leader == null)
                throw new NotLeadingException("Not a leader, cannot accept commands. " + _lastStateChangeReason);

            return leader.PutAsync(cmd);
        }

        public void SwitchToCandidateStateOnTimeout()
        {
            SwitchToCandidateState("Election timeout");
        }

        public void SwitchToCandidateState(string reason, bool forced = false)
        {
            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var clusterTopology = GetTopology(context);
                if (clusterTopology.TopologyId == null ||
                    clusterTopology.Voters.Contains(_url) == false)
                {
                    if (Log.IsInfoEnabled)
                    {
                        Log.Info("Can't switch to candidate mode when not initialized with topology / not a voter");
                    }
                    return;
                }
            }

            if (Log.IsInfoEnabled)
            {
                Log.Info("Switching to candidate state");
            }
            var candidate = new Candidate(this)
            {
                IsForcedElection = forced
            };
            SetNewState(State.Candidate, candidate, CurrentTerm, reason);
            candidate.Start();
        }

        public void DeleteTopology(TransactionOperationContext context)
        {
            var topology = GetTopology(context);
            var newTopology = new ClusterTopology(topology.TopologyId, null, new string[0], new string[0], new string[0]);
            SetTopology(context, newTopology);
        }

        public unsafe ClusterTopology GetTopology(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);
            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(TopologySlice);
            if (read == null)
                return new ClusterTopology(null, null, new string[0], new string[0], new string[0]);

            var json = new BlittableJsonReaderObject(read.Reader.Base, read.Reader.Length, context);
            return JsonDeserializationRachis<ClusterTopology>.Deserialize(json);
        }

        public unsafe BlittableJsonReaderObject GetTopologyRaw(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);
            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(TopologySlice);
            if (read == null)
                return null;

            return new BlittableJsonReaderObject(read.Reader.Base, read.Reader.Length, context);
        }

        public BlittableJsonReaderObject SetTopology(TransactionOperationContext context, ClusterTopology topology)
        {
            Debug.Assert(context.Transaction != null);
            var tx = context.Transaction.InnerTransaction;
            var topologyJson = SetTopology(this, tx, context, topology);

            return topologyJson;
        }

        private static BlittableJsonReaderObject SetTopology(RachisConsensus engine, Transaction tx,
            JsonOperationContext context, ClusterTopology topology)
        {
            var djv = new DynamicJsonValue
            {
                [nameof(ClusterTopology.TopologyId)] = topology.TopologyId,
                [nameof(ClusterTopology.ApiKey)] = topology.ApiKey,
                [nameof(ClusterTopology.Voters)] = new DynamicJsonArray(topology.Voters),
                [nameof(ClusterTopology.Promotables)] = new DynamicJsonArray(topology.Promotables),
                [nameof(ClusterTopology.NonVotingMembers)] = new DynamicJsonArray(topology.NonVotingMembers),
            };

            var topologyJson = context.ReadObject(djv, "topology");

            SetTopology(engine, tx, topologyJson);

            return topologyJson;
        }

        public static unsafe void SetTopology(RachisConsensus engine, Transaction tx,
            BlittableJsonReaderObject topologyJson)
        {
            var state = tx.CreateTree(GlobalStateSlice);
            byte* ptr;
            using (state.DirectAdd(TopologySlice, topologyJson.Size, out ptr))
            {
                topologyJson.CopyTo(ptr);
            }

            if (engine == null)
                return;


            tx.LowLevelTransaction.OnDispose += _ =>
            {
                Interlocked.Exchange(ref engine._topologyChanged, new TaskCompletionSource<object>()).TrySetResult(null);
            };


        }

        public string GetDebugInformation()
        {
            return _url;
        }

        /// <summary>
        /// This method is expected to run for a long time (lifetime of the connection)
        /// and can never throw. We expect this to be on a separate thread
        /// </summary>
        public void AcceptNewConnection(TcpClient tcpClient, Action<RachisHello> sayHello = null)
        {
            RemoteConnection remoteConnection = null;
            try
            {
                remoteConnection = new RemoteConnection(_url, tcpClient.GetStream());
                try
                {
                    RachisHello initialMessage;
                    ClusterTopology clusterTopology;
                    TransactionOperationContext context;
                    using (ContextPool.AllocateOperationContext(out context))
                    {
                        initialMessage = remoteConnection.InitFollower(context);
                        sayHello?.Invoke(initialMessage);
                        using (context.OpenReadTransaction())
                        {
                            clusterTopology = GetTopology(context);
                        }
                    }

                    if (initialMessage.TopologyId != clusterTopology.TopologyId &&
                        string.IsNullOrEmpty(clusterTopology.TopologyId) == false)
                    {
                        throw new InvalidOperationException(
                            $"{initialMessage.DebugSourceIdentifier} attempted to connect to us with topology id {initialMessage.TopologyId} but our topology id is already set ({clusterTopology.TopologyId}). " +
                            $"Rejecting connection from outside our cluster, this is likely an old server trying to connect to us.");
                    }

                    switch (initialMessage.InitialMessageType)
                    {
                        case InitialMessageType.RequestVote:
                            var elector = new Elector(this, remoteConnection);
                            elector.HandleVoteRequest();
                            break;
                        case InitialMessageType.AppendEntries:
                            var follower = new Follower(this, remoteConnection);
                            follower.TryAcceptConnection();
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("Uknown initial message value: " +
                                                                  initialMessage.InitialMessageType +
                                                                  ", no idea how to handle it");
                    }

                    //initialMessage.AppendEntries
                    // validate that can handle this
                    // start listening thread
                }
                catch (Exception e)
                {
                    TransactionOperationContext context;
                    using (ContextPool.AllocateOperationContext(out context))
                        remoteConnection.Send(context, e);
                }
            }
            catch (Exception e)
            {
                if (Log.IsInfoEnabled)
                {
                    Log.Info("Failed to process incoming connection", e);
                }

                try
                {
                    remoteConnection?.Dispose();
                }
                catch (Exception)
                {
                }

                try
                {
                    tcpClient?.Dispose();
                }
                catch (Exception)
                {
                }

            }
        }

        public unsafe long InsertToLeaderLog(TransactionOperationContext context, BlittableJsonReaderObject cmd,
            RachisEntryFlags flags)
        {
            Debug.Assert(context.Transaction != null);
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);

            long lastIndex;

            TableValueReader reader;
            if (table.SeekOnePrimaryKey(Slices.AfterAllKeys, out reader))
            {
                int size;
                lastIndex = Bits.SwapBytes(*(long*) reader.Read(0, out size));
                Debug.Assert(size == sizeof(long));
            }
            else
            {
                long lastIndexTerm;
                GetLastTruncated(context, out lastIndex, out lastIndexTerm);
            }
            lastIndex += 1;
            var tvb = new TableValueBuilder
            {
                Bits.SwapBytes(lastIndex),
                CurrentTerm,
                {cmd.BasePointer, cmd.Size},
                (int) flags
            };
            table.Insert(tvb);

            return lastIndex;
        }

        public unsafe void TruncateLogBefore(TransactionOperationContext context, long upto)
        {
            long lastIndex;
            long lastTerm;
            GetLastCommitIndex(context, out lastIndex, out lastTerm);

            long entryTerm;
            long entryIndex;

            if (lastIndex < upto)
            {
                upto = lastIndex; // max we can delete
                entryIndex = lastIndex;
                entryTerm = lastTerm;
            }
            else
            {
                var maybeTerm = GetTermFor(context, upto);
                if (maybeTerm == null)
                    return;
                entryIndex = upto;
                entryTerm = maybeTerm.Value;
            }
            GetLastTruncated(context, out lastIndex, out lastTerm);

            if (lastIndex >= upto)
                return;

            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            while (true)
            {
                TableValueReader reader;
                if (table.SeekOnePrimaryKey(Slices.BeforeAllKeys, out reader) == false)
                    break;

                int size;
                entryIndex = Bits.SwapBytes(*(long*) reader.Read(0, out size));
                if (entryIndex > upto)
                    break;
                Debug.Assert(size == sizeof(long));

                entryTerm = *(long*) reader.Read(1, out size);
                Debug.Assert(size == sizeof(long));

                table.Delete(reader.Id);
            }
            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
            byte* ptr;
            using (state.DirectAdd(LastTruncatedSlice, sizeof(long) * 2, out ptr))
            {
                var data = (long*) ptr;
                data[0] = entryIndex;
                data[1] = entryTerm;
            }
        }

        public unsafe BlittableJsonReaderObject AppendToLog(TransactionOperationContext context,
            List<RachisEntry> entries)
        {
            Debug.Assert(entries.Count > 0);
            Debug.Assert(context.Transaction != null);
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);

            long reversedEntryIndex = -1;

            BlittableJsonReaderObject lastTopology = null;

            Slice key;
            using (
                Slice.External(context.Transaction.InnerTransaction.Allocator, (byte*) &reversedEntryIndex, sizeof(long),
                    out key))
            {
                var lastEntryIndex = GetLastEntryIndex(context);
                var firstIndexInEntriesThatWeHaveNotSeen = 0;
                foreach (var entry in entries)
                {
                    if (entry.Index > lastEntryIndex)
                        break;

                    firstIndexInEntriesThatWeHaveNotSeen++;
                }
                var prevIndex = lastEntryIndex;

                for (var index = firstIndexInEntriesThatWeHaveNotSeen; index < entries.Count; index++)
                {
                    var entry = entries[index];
                    if (entry.Index != prevIndex + 1)
                    {
                        throw new InvalidOperationException(
                            $"Gap in the entries, prev was {prevIndex} but now trying {entry.Index}");
                    }

                    prevIndex = entry.Index;
                    reversedEntryIndex = Bits.SwapBytes(entry.Index);
                    TableValueReader reader;
                    if (table.ReadByKey(key, out reader)) // already exists
                    {
                        int size;
                        var term = *(long*) reader.Read(1, out size);
                        Debug.Assert(size == sizeof(long));
                        if (term == entry.Term)
                            continue; // same, can skip

                        // we have found a divergence in the log, and we now need to trucate it from this
                        // location forward

                        do
                        {
                            table.Delete(reader.Id);
                            // move to the next id, have to swap to little endian & back to get proper
                            // behavior
                            reversedEntryIndex = Bits.SwapBytes(Bits.SwapBytes(reversedEntryIndex) + 1);
                            // now we'll find the next item to delete, and do so until we run out of items 
                            // to write
                        } while (table.ReadByKey(key, out reader));
                    }

                    var nested = context.ReadObject(entry.Entry, "entry");
                    table.Insert(new TableValueBuilder
                    {
                        reversedEntryIndex,
                        entry.Term,
                        {nested.BasePointer, nested.Size},
                        (int) entry.Flags
                    });
                    if (entry.Flags == RachisEntryFlags.Topology)
                    {
                        lastTopology?.Dispose();
                        lastTopology = nested;
                    }
                    else
                    {
                        nested.Dispose();
                    }
                }
            }
            return lastTopology;
        }

        private static void GetLastTruncated(TransactionOperationContext context, out long lastTruncatedIndex,
            out long lastTruncatedTerm)
        {
            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(LastTruncatedSlice);
            if (read == null)
            {
                lastTruncatedIndex = 0;
                lastTruncatedTerm = 0;
                return;
            }
            var reader = read.Reader;
            lastTruncatedIndex = reader.ReadLittleEndianInt64();
            lastTruncatedTerm = reader.ReadLittleEndianInt64();
        }


        public unsafe BlittableJsonReaderObject GetEntry(TransactionOperationContext context, long index,
            out RachisEntryFlags flags)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            var reversedIndex = Bits.SwapBytes(index);
            Slice key;
            using (Slice.External(context.Allocator, (byte*) &reversedIndex, sizeof(long), out key))
            {
                TableValueReader reader;
                if (table.ReadByKey(key, out reader) == false)
                {
                    flags = RachisEntryFlags.Invalid;
                    return null;
                }
                int size;
                flags = *(RachisEntryFlags*) reader.Read(3, out size);
                Debug.Assert(size == sizeof(RachisEntryFlags));
                var ptr = reader.Read(2, out size);
                return new BlittableJsonReaderObject(ptr, size, context);
            }
        }

        public long GetLastCommitIndex(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);

            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(LastCommitSlice);
            if (read == null)
                return 0;
            return read.Reader.ReadLittleEndianInt64();
        }

        public void GetLastCommitIndex(TransactionOperationContext context, out long index, out long term)
        {
            Debug.Assert(context.Transaction != null);

            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(LastCommitSlice);
            if (read == null)
            {
                index = 0;
                term = 0;
                return;
            }
            var reader = read.Reader;
            index = reader.ReadLittleEndianInt64();
            term = reader.ReadLittleEndianInt64();
        }



        public unsafe void SetLastCommitIndex(TransactionOperationContext context, long index, long term)
        {
            Debug.Assert(context.Transaction != null);

            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(LastCommitSlice);
            if (read != null)
            {
                var reader = read.Reader;
                var oldIndex = reader.ReadLittleEndianInt64();
                if (oldIndex > index)
                    throw new InvalidOperationException(
                        $"Cannot reduce the last commit index (is {oldIndex} but was requested to reduce to {index})");
                if (oldIndex == index)
                {
                    var oldTerm = reader.ReadLittleEndianInt64();
                    if (oldTerm != term)
                        throw new InvalidOperationException(
                            $"Cannot change just the last commit index (is {oldIndex} term, was {oldTerm} but was requested to change it to {term})");
                }
            }

            byte* ptr;
            using (state.DirectAdd(LastCommitSlice, sizeof(long) * 2, out ptr))
            {
                var data = (long*) ptr;
                data[0] = index;
                data[1] = term;
            }

            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += _ =>
            {
                Interlocked.Exchange(ref _commitIndexChanged, new TaskCompletionSource<object>()).TrySetResult(null);
            };
        }

        public async Task WaitForCommitIndexChange(CommitIndexModification modification, long value)
        {
            while (true)
            {
                var task = _commitIndexChanged.Task;
                TransactionOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                using (context.OpenReadTransaction())
                {
                    var commitIndex = GetLastCommitIndex(context);
                    switch (modification)
                    {
                        case CommitIndexModification.Equal:
                            if (value == commitIndex)
                                return;
                            break;
                        case CommitIndexModification.GreaterOrEqual:
                            if (value <= commitIndex)
                                return;
                            break;
                        case CommitIndexModification.AnyChange:
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(modification), modification, null);
                    }
                }
                await task;
            }
        }

        public unsafe Tuple<long, long> GetLogEntriesRange(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);

            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            TableValueReader reader;
            if (table.SeekOnePrimaryKey(Slices.AfterAllKeys, out reader) == false)
                return Tuple.Create(0L, 0L);
            int size;
            var max = Bits.SwapBytes(*(long*) reader.Read(0, out size));
            Debug.Assert(size == sizeof(long));
            if (table.SeekOnePrimaryKey(Slices.BeforeAllKeys, out reader) == false)
                return Tuple.Create(0L, 0L);
            var min = Bits.SwapBytes(*(long*) reader.Read(0, out size));
            Debug.Assert(size == sizeof(long));

            return Tuple.Create(min, max);
        }

        public unsafe long GetFirstEntryIndex(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);

            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            TableValueReader reader;
            if (table.SeekOnePrimaryKey(Slices.BeforeAllKeys, out reader) == false)
            {
                long lastTruncatedIndex;
                long _;
                GetLastTruncated(context, out lastTruncatedIndex, out _);
                return lastTruncatedIndex;
            }
            int size;
            var max = Bits.SwapBytes(*(long*) reader.Read(0, out size));
            Debug.Assert(size == sizeof(long));
            return max;
        }

        public unsafe long GetLastEntryIndex(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);

            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            TableValueReader reader;
            if (table.SeekOnePrimaryKey(Slices.AfterAllKeys, out reader) == false)
            {
                long lastTruncatedIndex;
                long _;
                GetLastTruncated(context, out lastTruncatedIndex, out _);
                return lastTruncatedIndex;
            }
            int size;
            var max = Bits.SwapBytes(*(long*) reader.Read(0, out size));
            Debug.Assert(size == sizeof(long));
            return max;
        }

        public long GetTermForKnownExisting(TransactionOperationContext context, long index)
        {
            var termFor = GetTermFor(context, index);
            if (termFor == null)
                throw new InvalidOperationException("Expected the index " + index +
                                                    " to have a term in the entries, but got null");
            return termFor.Value;
        }

        public unsafe long? GetTermFor(TransactionOperationContext context, long index)
        {
            Debug.Assert(context.Transaction != null);
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            var reversedIndex = Bits.SwapBytes(index);
            Slice key;
            using (
                Slice.External(context.Transaction.InnerTransaction.Allocator, (byte*) &reversedIndex, sizeof(long),
                    out key))
            {
                TableValueReader reader;
                if (table.ReadByKey(key, out reader) == false)
                {
                    long lastIndex;
                    long lastTerm;
                    GetLastCommitIndex(context, out lastIndex, out lastTerm);
                    if (lastIndex == index)
                        return lastTerm;
                    GetLastTruncated(context, out lastIndex, out lastTerm);
                    if (lastIndex == index)
                        return lastTerm;
                    return null;
                }
                int size;
                var term = *(long*) reader.Read(1, out size);
                Debug.Assert(size == sizeof(long));
                return term;
            }
        }

        public void FoundAboutHigherTerm(long term)
        {
            if (term == CurrentTerm)
                return;

            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    // we check it here again because now we are under the tx lock, so we can't get into concurrency issues
                    if (term == CurrentTerm)
                        return;

                    CastVoteInTerm(context, term, votedFor: null);

                    tx.Commit();
                }
            }
        }

        public unsafe void CastVoteInTerm(TransactionOperationContext context, long term, string votedFor)
        {
            Debug.Assert(context.Transaction != null);
            if (term <= CurrentTerm)
                throw new ConcurrencyException($"The current term {CurrentTerm} is larger than {term}, aborting change");

            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
            byte* ptr;
            using (state.DirectAdd(CurrentTermSlice, sizeof(long), out ptr))
            {
                *(long*) ptr = term;
            }

            votedFor = votedFor ?? String.Empty;

            var size = Encoding.UTF8.GetByteCount(votedFor);

            using (state.DirectAdd(VotedForSlice, size, out ptr))
            {
                fixed (char* pVotedFor = votedFor)
                {
                    Encoding.UTF8.GetBytes(pVotedFor, votedFor.Length, ptr, size);
                }
            }

            CurrentTerm = term;
        }

        public string GetWhoGotMyVoteIn(TransactionOperationContext context, long term)
        {
            Debug.Assert(context.Transaction != null);

            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
            var read = state.Read(CurrentTermSlice);

            var votedTerm = read?.Reader.ReadLittleEndianInt64();

            if (votedTerm != term)
                return null;

            read = state.Read(VotedForSlice);

            return read?.Reader.ReadString(read.Reader.Length);
        }

        public event EventHandler OnDispose;

        public virtual void Dispose()
        {
            OnDispose?.Invoke(this, EventArgs.Empty);
            ThreadPool.QueueUserWorkItem(_ => _topologyChanged.TrySetCanceled());
            ContextPool?.Dispose();
            _persistentState?.Dispose();
            _options?.Dispose();
        }

        public Stream ConenctToPeer(string url, string apiKey)
        {
            //var serverUrl = new UriBuilder(url)
            //{
            //    Fragment = null // remove debug info
            //}.Uri.ToString();

            //var tcpInfo = ReplicationUtils.GetTcpInfo(serverUrl, null, apiKey);

            var tcpInfo = new Uri(url);
            var tcpClient = new TcpClient();
            tcpClient.ConnectAsync(tcpInfo.Host, tcpInfo.Port).Wait();
            return tcpClient.GetStream();
        }

        public static void Bootstarp(StorageEnvironmentOptions options, string self)
        {
            var old = options.OwnsPagers;
            options.OwnsPagers = false;
            try
            {
                using (var env = new StorageEnvironment(options))
                {
                    using (var tx = env.WriteTransaction())
                    using (var ctx = JsonOperationContext.ShortTermSingleUse())
                    {
                        var topology = new ClusterTopology(Guid.NewGuid().ToString(),
                            null,
                            new string[] {self},
                            new string[0],
                            new string[0]);

                        SetTopology(null, tx, ctx, topology);

                        tx.Commit();
                    }
                }
            }
            finally
            {
                options.OwnsPagers = old;
            }
        }

        public Task AddToClusterAsync(string node)
        {
            return ModifyTopologyAsync(node, Leader.TopologyModification.Promotable,true);
        }
    
        public Task RemoveFromClusterAsync(string node)
        {
            return ModifyTopologyAsync(node, Leader.TopologyModification.Remove);
        }

        private async Task ModifyTopologyAsync(string newNode, Leader.TopologyModification modification,bool validateNotInTopology = false)
        {
            var leader = _currentLeader;
            if (leader == null)
                throw new NotLeadingException("Not a leader, cannot accept commands. " + _lastStateChangeReason);

            Task task;
            while (leader.TryModifyTopology(newNode, modification, out task, validateNotInTopology) == false)
                await task;

            await task;
        }

        private volatile string _leaderUrl;
        public string LeaderUrl
        {
            get
            {
                switch (CurrentState)
                {
                    case State.Passive:
                        return string.Empty;
                    case State.Candidate:
                        return "<me, I hope?>";
                    case State.Follower:
                        return _leaderUrl;
                    case State.LeaderElect:
                        return Url;
                    case State.Leader:
                        return Url;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            internal set
            {
                _leaderUrl = value;
            }
        }
        public abstract bool ShouldSnapshot(Slice slice, RootObjectType type);

        public abstract void Apply(TransactionOperationContext context, long uptoInclusive);

        public abstract void SnapshotInstalled(TransactionOperationContext context);
    }

    public class NotLeadingException : Exception
    {
        public NotLeadingException()
        {
        }

        public NotLeadingException(string message) : base(message)
        {
        }

        public NotLeadingException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}