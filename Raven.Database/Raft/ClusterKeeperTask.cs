// -----------------------------------------------------------------------
//  <copyright file="ClusterKeeperTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Rachis;
using Rachis.Commands;
using Rachis.Transport;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Commercial;
using Raven.Database.Plugins;
using Raven.Database.Raft.Dto;
using Raven.Database.Raft.Util;
using Raven.Database.Server;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Raft
{
    public class ClusterKeeperTask : IServerStartupTask
    {
        private DocumentDatabase systemDatabase;

        private ClusterManager clusterManager;
        private RavenDBOptions options;

        public void Execute(RavenDBOptions serverOptions)
        {
            if (IsValidLicense() == false)
                return;

            systemDatabase = serverOptions.SystemDatabase;
            serverOptions.DatabaseLandlord.ForAllDatabases(DisableReplicationTask);
            clusterManager = serverOptions.ClusterManager.Value = ClusterManagerFactory.Create(systemDatabase, serverOptions.DatabaseLandlord);
            options = serverOptions;
            systemDatabase.Notifications.OnDocumentChange += (db, notification, metadata) =>
            {
                if (string.Equals(notification.Id, Constants.Cluster.ClusterConfigurationDocumentKey, StringComparison.OrdinalIgnoreCase))
                {
                    if (notification.Type != DocumentChangeTypes.Put)
                        return;

                    HandleClusterConfigurationChanges();
                }
            };

            clusterManager.Engine.TopologyChanged += HandleTopologyChanges;
            clusterManager.Engine.StateChanged += HandleStateChanges;
            options.DatabaseLandlord.OnDatabaseLoaded += HandleDatabaseLoaded;
            HandleClusterConfigurationChanges();

        }

        private void HandleDatabaseLoaded(string dbName)
        {
            Task.Run(delegate
            {
                var db = options.DatabaseLandlord.GetResourceInternal(dbName).GetAwaiter().GetResult();
                if (db == null) return;
                var shouldActivate = clusterManager.Engine.State == RaftEngineState.Leader;
                SetReplicationTaskState(db, shouldActivate);
            });
        }

        private void HandleStateChanges(RaftEngineState state)
        {
            switch (state)
            {
                case RaftEngineState.Leader:
                    options.DatabaseLandlord.ForAllDatabases(ActivateReplicationTask);
                    break;
                default:
                    options.DatabaseLandlord.ForAllDatabases(DisableReplicationTask);
                    break;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DisableReplicationTask(DocumentDatabase db)
        {
            SetReplicationTaskState(db, false);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ActivateReplicationTask(DocumentDatabase db)
        {
            SetReplicationTaskState(db, true);
        }

        private void SetReplicationTaskState(DocumentDatabase db, bool activate)
        {
            var replicationTask = db.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();
            if (replicationTask == null) return;
            if(activate)
                replicationTask.Continue();
            else
                replicationTask.Pause();
        }
        private static bool IsValidLicense()
        {
            DevelopmentHelper.TimeBomb();
            return true;

            if (ValidateLicense.CurrentLicense.IsCommercial == false)
                return false;

            string value;
            if (ValidateLicense.CurrentLicense.Attributes.TryGetValue("cluster", out value) == false)
                return false;

            bool cluster;
            if (bool.TryParse(value, out cluster) == false)
                return false;

            return cluster;
        }

        public void Dispose()
        {
            if (clusterManager != null)
                clusterManager.Engine.TopologyChanged -= HandleTopologyChanges;
        }

        private void HandleTopologyChanges(TopologyChangeCommand command)
        {
            if (RaftHelper.HasDifferentNodes(command) == false)
                return;

            if (command.Previous == null)
            {
                HandleClusterConfigurationChanges();
                return;
            }

            var removedNodeUrls = command.Previous.AllNodes.Select(x => x.Uri.AbsoluteUri).Except(command.Requested.AllNodes.Select(x => x.Uri.AbsoluteUri)).ToList();

            HandleClusterConfigurationChanges(removedNodeUrls);
        }

        private void HandleClusterConfigurationChanges(List<string> removedNodeUrls = null)
        {
            var configurationJson = systemDatabase.Documents.Get(Constants.Cluster.ClusterConfigurationDocumentKey, null);
            if (configurationJson == null)
                return;

            var configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();

            HandleClusterReplicationChanges(removedNodeUrls, configuration.EnableReplication);
        }

        private void HandleClusterReplicationChanges(List<string> removedNodes, bool enableReplication)
        {
            var currentTopology = clusterManager.Engine.CurrentTopology;
            var replicationDocumentJson = systemDatabase.Documents.Get(Constants.Global.ReplicationDestinationsDocumentName, null);
            var replicationDocument = replicationDocumentJson != null ? replicationDocumentJson.DataAsJson.JsonDeserialization<ReplicationDocument>() : new ReplicationDocument();

            var replicationDocumentNormalizedDestinations = replicationDocument.Destinations.ToDictionary(x => RaftHelper.GetNormalizedNodeUrl(x.Url), x => x);

            var currentTopologyNormalizedDestionations = currentTopology.AllNodes.ToDictionary(x => x.Uri.AbsoluteUri.ToLowerInvariant(), x => x);

            var urls = replicationDocumentNormalizedDestinations.Keys.Union(currentTopologyNormalizedDestionations.Keys).ToList();

            foreach (var url in urls)
            {
                ReplicationDestination destination;
                replicationDocumentNormalizedDestinations.TryGetValue(url, out destination);
                NodeConnectionInfo node;
                currentTopologyNormalizedDestionations.TryGetValue(url, out node);

                if (destination == null && node == null)
                    continue; // not possible, but...

                if (destination != null && node == null)
                {
                    if (removedNodes.Contains(url, StringComparer.OrdinalIgnoreCase) == false)
                        continue; // external destination

                    replicationDocument.Destinations.Remove(destination);
                    continue;
                }

                if (string.Equals(node.Name, clusterManager.Engine.Options.SelfConnection.Name, StringComparison.OrdinalIgnoreCase))
                    continue; // skipping self

                if (destination == null)
                {
                    destination = new ReplicationDestination();
                    replicationDocument.Destinations.Add(destination);
                }

                destination.ApiKey = node.ApiKey;
                destination.Database = null;
                destination.Disabled = enableReplication == false;
                destination.Domain = node.Domain;
                destination.Password = node.Password;
                destination.TransitiveReplicationBehavior = TransitiveReplicationOptions.Replicate;
                destination.SkipIndexReplication = false;
                destination.Url = node.Uri.AbsoluteUri;
                destination.Username = node.Username;
            }

            systemDatabase.Documents.Put(Constants.Global.ReplicationDestinationsDocumentName, null, RavenJObject.FromObject(replicationDocument), new RavenJObject(), null);
        }
    }
}
