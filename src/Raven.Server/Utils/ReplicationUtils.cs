﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Commands;
using Raven.Client.Data;
using Raven.Client.Http;
using Raven.Client.Replication;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;

namespace Raven.Server.Utils
{
    public static class ReplicationUtils
    {
        public static NodeTopologyInfo GetLocalTopology(
            DocumentDatabase database,
            ReplicationDocument replicationDocument)
        {
            var topologyInfo = new NodeTopologyInfo { DatabaseId = database.DbId.ToString() };
            topologyInfo.InitializeOSInformation();

            var replicationLoader = database.DocumentReplicationLoader;

            GetLocalIncomingTopology(replicationLoader, topologyInfo);

            foreach (var destination in replicationDocument.Destinations)
            {
                OutgoingReplicationHandler outgoingHandler;
                DocumentReplicationLoader.ConnectionShutdownInfo connectionFailureInfo;

                if (TryGetActiveDestination(destination, replicationLoader.OutgoingHandlers, out outgoingHandler))
                {
                    
                    topologyInfo.Outgoing.Add(
                        new ActiveNodeStatus
                        {
                            DbId = outgoingHandler.DestinationDbId,
                            IsCurrentlyConnected = true,
                            Database = destination.Database,
                            Url = destination.Url,
                            SpecifiedCollections = destination.SpecifiedCollections ?? new Dictionary<string, string>(),
                            LastDocumentEtag = outgoingHandler._lastSentDocumentEtag,
                            LastIndexTransformerEtag = outgoingHandler._lastSentIndexOrTransformerEtag,
                            LastHeartbeatTicks = outgoingHandler.LastHeartbeatTicks,
                            NodeStatus = ActiveNodeStatus.Status.Online
                        });

                }
                else if (replicationLoader.OutgoingFailureInfo.TryGetValue(destination, out connectionFailureInfo))
                {
                    topologyInfo.Offline.Add(
                        new InactiveNodeStatus
                        {
                            Database = destination.Database,
                            Url = destination.Url,
                            Exception = connectionFailureInfo.LastException?.ToString(),
                            Message = connectionFailureInfo.LastException?.Message
                        });
                }
                else
                {
                    topologyInfo.Offline.Add(
                        new InactiveNodeStatus
                        {
                            Database = destination.Database,
                            Url = destination.Url,
                            Exception = destination.Disabled ? "Replication destination has been disabled" : null
                        });
                }
            }

            return topologyInfo;
        }

        public static void GetLocalIncomingTopology(DocumentReplicationLoader replicationLoader, NodeTopologyInfo topologyInfo)
        {
            foreach (var incomingHandler in replicationLoader.IncomingHandlers)
            {
                topologyInfo.Incoming.Add(
                    new ActiveNodeStatus
                    {
                        DbId = incomingHandler.ConnectionInfo.SourceDatabaseId,
                        Database = incomingHandler.ConnectionInfo.SourceDatabaseName,
                        Url = new UriBuilder(incomingHandler.ConnectionInfo.SourceUrl)
                        {
                            Host = incomingHandler.ConnectionInfo.RemoteIp
                        }.Uri.ToString(),
                        IsCurrentlyConnected = true,
                        NodeStatus = ActiveNodeStatus.Status.Online,
                        LastDocumentEtag = incomingHandler.LastDocumentEtag,
                        LastIndexTransformerEtag = incomingHandler.LastIndexOrTransformerEtag,
                        LastHeartbeatTicks = incomingHandler.LastHeartbeatTicks
                    });
            }
        }

        public static bool TryGetActiveDestination(ReplicationDestination destination,
            IEnumerable<OutgoingReplicationHandler> outgoingReplicationHandlers,
            out OutgoingReplicationHandler handler)
        {
            handler = null;
            foreach (var outgoing in outgoingReplicationHandlers)
            {
                if (outgoing.Destination.Url.Equals(destination.Url, StringComparison.OrdinalIgnoreCase) &&
                    outgoing.Destination.Database.Equals(destination.Database, StringComparison.OrdinalIgnoreCase))
                {
                    handler = outgoing;
                    return true;
                }
            }

            return false;
        }

        public static async Task<TcpConnectionInfo> GetTcpInfoAsync(JsonOperationContext context,
        string url,
        string databaseName,
        string apiKey)
        {
            using (var requestExecuter = new RequestExecuter(url, databaseName, apiKey))
            {
                var getTcpInfoCommand = new GetTcpInfoCommand();
                await requestExecuter.ExecuteAsync(getTcpInfoCommand, context);

                return getTcpInfoCommand.Result;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string ChangeVectorToString(Dictionary<Guid, long> changeVector)
        {
            var sb = new StringBuilder();
            foreach (var kvp in changeVector)
                sb.Append($"{kvp.Key}:{kvp.Value};");

            return sb.ToString();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string ChangeVectorToString(ChangeVectorEntry[] changeVector)
        {
            var sb = new StringBuilder();
            foreach (var kvp in changeVector)
                sb.Append($"{kvp.DbId}:{kvp.Etag};");

            return sb.ToString();
        }


        public static unsafe void WriteChangeVectorTo(DocumentsOperationContext context, Dictionary<Guid, long> changeVector, Tree tree)
        {
            Guid dbId;
            long etagBigEndian;
            Slice keySlice;
            Slice valSlice;
            using (Slice.External(context.Allocator, (byte*)&dbId, sizeof(Guid), out keySlice))
            using (Slice.External(context.Allocator, (byte*)&etagBigEndian, sizeof(long), out valSlice))
            {
                foreach (var kvp in changeVector)
                {
                    dbId = kvp.Key;
                    etagBigEndian = Bits.SwapBytes(kvp.Value);
                    tree.Add(keySlice, valSlice);
                }
            }
        }

        public static unsafe void WriteChangeVectorTo(ByteStringContext context, Dictionary<Guid, long> changeVector, Tree tree)
        {
            Guid dbId;
            long etagBigEndian;
            Slice keySlice;
            Slice valSlice;
            using (Slice.External(context, (byte*)&dbId, sizeof(Guid), out keySlice))
            using (Slice.External(context, (byte*)&etagBigEndian, sizeof(long), out valSlice))
            {
                foreach (var kvp in changeVector)
                {
                    dbId = kvp.Key;
                    etagBigEndian = Bits.SwapBytes(kvp.Value);
                    tree.Add(keySlice, valSlice);
                }
            }
        }

        public static unsafe TEnum GetEnumFromTableValueReader<TEnum>(ref TableValueReader tvr, int index)
        {
            int size;
            var storageTypeNum = *(int*)tvr.Read(index, out size);
            return (TEnum)Enum.ToObject(typeof(TEnum), storageTypeNum);
        }

        public static unsafe ChangeVectorEntry[] GetChangeVectorEntriesFromTableValueReader(ref TableValueReader tvr, int index)
        {
            int size;
            var pChangeVector = (ChangeVectorEntry*)tvr.Read(index, out size);
            var changeVector = new ChangeVectorEntry[size / sizeof(ChangeVectorEntry)];
            for (int i = 0; i < changeVector.Length; i++)
            {
                changeVector[i] = pChangeVector[i];
            }
            return changeVector;
        }

        public static unsafe ChangeVectorEntry[] ReadChangeVectorFrom(Tree tree)
        {
            var changeVector = new ChangeVectorEntry[tree.State.NumberOfEntries];
            using (var iter = tree.Iterate(false))
            {
                if (iter.Seek(Slices.BeforeAllKeys) == false)
                    return changeVector;
                var buffer = new byte[sizeof(Guid)];
                int index = 0;
                do
                {
                    var read = iter.CurrentKey.CreateReader().Read(buffer, 0, sizeof(Guid));
                    if (read != sizeof(Guid))
                        throw new InvalidDataException($"Expected guid, but got {read} bytes back for change vector");

                    changeVector[index].DbId = new Guid(buffer);
                    changeVector[index].Etag = iter.CreateReaderForCurrent().ReadBigEndianInt64();
                    index++;
                } while (iter.MoveNext());
            }
            return changeVector;
        }

        public static ChangeVectorEntry[] GetChangeVectorForWrite(ChangeVectorEntry[] existingChangeVector, Guid dbid, long etag)
        {
            if (existingChangeVector == null || existingChangeVector.Length == 0)
            {
                return new[]
                {
                    new ChangeVectorEntry
                    {
                        DbId = dbid,
                        Etag = etag
                    }
                };
            }

            return UpdateChangeVectorWithNewEtag(dbid, etag, existingChangeVector);
        }

        public static ChangeVectorEntry[] UpdateChangeVectorWithNewEtag(Guid dbId, long newEtag, ChangeVectorEntry[] changeVector)
        {
            var length = changeVector.Length;
            for (int i = 0; i < length; i++)
            {
                if (changeVector[i].DbId == dbId)
                {
                    changeVector[i].Etag = newEtag;
                    return changeVector;
                }
            }
            Array.Resize(ref changeVector, length + 1);
            changeVector[length].DbId = dbId;
            changeVector[length].Etag = newEtag;
            return changeVector;
        }

        public static ChangeVectorEntry[] MergeVectors(ChangeVectorEntry[] vectorA, ChangeVectorEntry[] vectorB)
        {
            Array.Sort(vectorA);
            Array.Sort(vectorB);
            int ia = 0, ib = 0;
            var merged = new List<ChangeVectorEntry>();
            while(ia < vectorA.Length && ib < vectorB.Length)
            {
                int res = vectorA[ia].CompareTo(vectorB[ib]);
                if (res == 0)
                {
                    merged.Add(new ChangeVectorEntry
                    {
                        DbId = vectorA[ia].DbId,
                        Etag = Math.Max(vectorA[ia].Etag, vectorB[ib].Etag)
                    });
                    ia++;
                    ib++;
                }
                else if (res < 0)
                {
                    merged.Add(vectorA[ia]);
                    ia++;
                }
                else
                {
                    merged.Add(vectorB[ib]);
                    ib++;
                }
            }
            for (; ia < vectorA.Length; ia++)
            {
                merged.Add(vectorA[ia]);
            }
            for (; ib < vectorB.Length; ib++)
            {
                merged.Add(vectorB[ib]);
            }
            return merged.ToArray();
        }



        public static ChangeVectorEntry[] MergeVectors(IReadOnlyList<ChangeVectorEntry[]> changeVectors)
        {
            var mergedVector = new Dictionary<Guid, long>();

            foreach (var changeVector in changeVectors)
            {
                foreach (var changeVectorEntry in changeVector)
                {
                    if (!mergedVector.ContainsKey(changeVectorEntry.DbId))
                    {
                        mergedVector[changeVectorEntry.DbId] = changeVectorEntry.Etag;
                    }
                    else
                    {
                        mergedVector[changeVectorEntry.DbId] = Math.Max(mergedVector[changeVectorEntry.DbId],
                            changeVectorEntry.Etag);
                    }
                }
            }
            
            return mergedVector.Select(kvp => new ChangeVectorEntry
            {
                DbId = kvp.Key,
                Etag = kvp.Value
            }).ToArray();
        }

        public static void EnsureCollectionTag(BlittableJsonReaderObject obj, string collection)
        {
            string actualCollection;
            BlittableJsonReaderObject metadata;
            if (obj.TryGet(Constants.Metadata.Key, out metadata) == false ||
                metadata.TryGet(Constants.Metadata.Collection, out actualCollection) == false ||
                actualCollection != collection)
            {
                if (collection == CollectionName.EmptyCollection)
                    return;

                ThrowInvalidCollectionAfterResolve(collection, null);
            }
        }

        private static void ThrowInvalidCollectionAfterResolve(string collection, string actual)
        {
            throw new InvalidOperationException(
                "Resolving script did not setup the appropriate '@collection'. Expeted " + collection + " but got " +
                actual);
        }
    }
}