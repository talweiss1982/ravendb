﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Lucene.Net.Support;
using Xunit;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;

namespace SlowTests.Server.Rachis
{
    public class TopologyChangesTests : RachisConsensusTestBase
    {
        [Fact]
        public async Task CanEnforceTopologyOnOldLeader()
        {
            var leader = await CreateNetworkAndGetLeader(3);
            var followers = GetFollowers();
            DisconnectFromNode(leader);
            var newServer = SetupServer();
            await leader.AddToClusterAsync(newServer.Url);
            await newServer.WaitForTopology(Leader.TopologyModification.Promotable);
            var newLeader = WaitForAnyToBecomeLeader(followers);
            Assert.NotNull(newLeader);
            ReconnectToNode(leader);
            Assert.True(leader.WaitForTopology(Leader.TopologyModification.Remove, newServer.Url).Wait(leader.ElectionTimeoutMs * 3));
        }
        /// <summary>
        /// This test checks that a node could be added to the cluster even if the node is down.
        /// We mimic a node been down by giving a url that doesn't exists.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task New_node_can_be_added_even_if_it_is_down()
        {
            var leader = await CreateNetworkAndGetLeader(3);
            Assert.True(leader.AddToClusterAsync("http://rachis.example.com:1337").Wait(leader.ElectionTimeoutMs*2),"non existing node should be able to join the cluster");
            List<Task> waitingList = new List<Task>();
            foreach (var consensus in RachisConsensuses)
            {
                waitingList.Add(consensus.WaitForTopology(Leader.TopologyModification.Promotable, "http://rachis.example.com:1337"));
            }
            Assert.True(Task.WhenAll(waitingList).Wait(leader.ElectionTimeoutMs * 2),"Cluster was non informed about new node within two election periods");
        }

        /// <summary>
        /// This test creates two nodes that don't exists and then setup those two nodes and make sure they are been updated with the current log.
        /// </summary>
        [Fact]
        public async Task Adding_additional_node_that_goes_offline_and_then_online_should_still_work()
        {
            var node4 = SetupServer(false, 53899);
            var node5 = SetupServer(false, 53898);
            DisconnectFromNode(node4);
            DisconnectFromNode(node5);
            var leader = await CreateNetworkAndGetLeader(3);            
            Assert.True(leader.AddToClusterAsync(node4.Url).Wait(leader.ElectionTimeoutMs * 2), "non existing node should be able to join the cluster");
            Assert.True(leader.AddToClusterAsync(node5.Url).Wait(leader.ElectionTimeoutMs * 2), "non existing node should be able to join the cluster");
            var t = IssueCommandsAndWaitForCommit(leader, 3, "test", 1);
            Assert.True(t.Wait(leader.ElectionTimeoutMs * 2),"Commands were not committed in time although there is a majority of active nodes in the cluster");

            ReconnectToNode(node4);
            ReconnectToNode(node5);

            Assert.True(node4.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.Equal, t.Result).Wait(leader.ElectionTimeoutMs * 2),
                "#D server didn't get the commands in time");
            Assert.True(node5.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.Equal, t.Result).Wait(leader.ElectionTimeoutMs * 2),
                "#E server didn't get the commands in time");
        }

        [Fact]
        public async Task Adding_already_existing_node_should_throw()
        {
            var leader = await CreateNetworkAndGetLeader(3);
            Assert.True(leader.AddToClusterAsync("http://not-a-real-url.com").Wait(leader.ElectionTimeoutMs * 2),
                "non existing node should be able to join the cluster");
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => leader.AddToClusterAsync("http://not-a-real-url.com"));
        }


        [Fact]
        public async Task Removal_of_non_existing_node_should_throw()
        {
            var leader = await CreateNetworkAndGetLeader(3);
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => leader.RemoveFromClusterAsync("http://not-a-real-url.com"));
        }

        [Theory]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(7)]
        public async Task Non_leader_Node_removed_from_cluster_should_update_peers_list(int nodeCount)
        {
            var leader = await CreateNetworkAndGetLeader(nodeCount);
            var follower = GetRandomFollower();
            Assert.True(leader.RemoveFromClusterAsync(follower.Url).Wait(leader.ElectionTimeoutMs*10),"Was unable to remove node from cluster in time");
            foreach (var node in RachisConsensuses)
            {
                if(node.Url == follower.Url)
                    continue;
                Assert.True(node.WaitForTopology(Leader.TopologyModification.Remove, follower.Url).Wait(node.ElectionTimeoutMs * 10),"Node was not removed from topology in time");
            }
        }
    }
}
