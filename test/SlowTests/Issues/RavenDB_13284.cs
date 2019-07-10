﻿using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Server.Config;
using Xunit;
using Raven.Tests.Core.Utils.Entities;

namespace SlowTests.Issues
{
    public class RavenDB_13284 : ReplicationTestBase
    {
        [Fact]
        public async Task ExternalReplicationCanReestablishAfterServerRestarts()
        {
            var serverSrc = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false
            });
            var serverDst = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false
            });
            using (var storeSrc = GetDocumentStore(new Options
            {
                Server = serverSrc,
                Path = Path.Combine(serverSrc.Configuration.Core.DataDirectory.FullPath, "ExternalReplicationCanReestablishAfterServerRestarts")
            }))
            using (var storeDst = GetDocumentStore(new Options
            {
                Server = serverDst,
                Path = Path.Combine(serverDst.Configuration.Core.DataDirectory.FullPath, "ExternalReplicationCanReestablishAfterServerRestarts")
            }))
            {
                await SetupReplicationAsync(storeSrc, storeDst);
                using (var session = storeSrc.OpenSession())
                {
                    session.Store(new User(), "user/1");
                    session.SaveChanges();
                }
                Assert.True(WaitForDocument(storeDst, "user/1"));

                // Taking down destination server
                var serverPath = serverDst.Configuration.Core.DataDirectory.FullPath;
                var nodePath = serverPath.Split('/').Last();
                var url = serverDst.WebUrl;
                await DisposeServerAndWaitForFinishOfDisposalAsync(serverDst);
                var settings = new Dictionary<string, string>
                {
                    {RavenConfiguration.GetKey(x => x.Core.ServerUrls), url}
                };
                // Put document while destination is down
                using (var session = storeSrc.OpenSession())
                {
                    session.Store(new User(), "user/2");
                    session.SaveChanges();
                }

                // Bring destination server up
                serverDst = GetNewServer(new ServerCreationOptions{RunInMemory = false, DeletePrevious = false, PartialPath = nodePath, CustomSettings = settings});

                Assert.True(WaitForDocument(storeDst, "user/2"));
            }
        }
    }
}
