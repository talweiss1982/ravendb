﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Server.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using DatabaseInfo = Raven.Client.Server.Operations.DatabaseInfo;

namespace Raven.Server.Web.System
{
    public class DatabasesHandler : RequestHandler
    {
        [RavenAction("/databases", "GET")]
        public Task Databases()
        {
            var dbName = GetQueryStringValue("info");
            if (dbName != null)
                return DbInfo(dbName);

            var namesOnly = GetBoolValueQueryString("namesOnly", required: false) ?? false;

            //TODO: fill all required information (see: RavenDB-5438) - return Raven.Client.Data.DatabasesInfo
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(DatabasesInfo.Databases));
                    writer.WriteArray(context, ServerStore.StartingWith(context, Constants.Documents.Prefix, GetStart(), GetPageSize(int.MaxValue)), (w, c, dbDoc) =>
                    {
                        var databaseName = dbDoc.Key.Substring(Constants.Documents.Prefix.Length);
                        if (namesOnly)
                        {
                            w.WriteString(databaseName);
                            return;
                        }

                        WriteDatabaseInfo(databaseName, dbDoc.Data, context, w);
                    }); 

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        private Task DbInfo(string dbName)
        {
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var dbId = Constants.Documents.Prefix + dbName;
                    long etag;
                    using (var dbDoc = ServerStore.Read(context, dbId, out etag))
                    {
                        WriteDatabaseInfo(dbName, dbDoc, context, writer);
                        return Task.CompletedTask;
                    }
                }
            }
        }

        private void WriteDatabaseInfo(string databaseName, BlittableJsonReaderObject data,
            TransactionOperationContext context, BlittableJsonTextWriter writer)
        {
            bool disabled;
            data.TryGet("Disabled", out disabled);

            Task<DocumentDatabase> dbTask;
            var online =
                ServerStore.DatabasesLandlord.ResourcesStoresCache.TryGetValue(databaseName, out dbTask) &&
                dbTask != null && dbTask.IsCompleted;

            if (dbTask != null && dbTask.IsFaulted)
            {
                WriteFaultedDatabaseInfo(context, writer, dbTask, databaseName);
                return;
            }

            var db = online ? dbTask.Result : null;

            var indexingStatus = db != null
                ? db.IndexStore.Status.ToString()
                : "Running";

            //Looking for disabled indexing flag inside the database settings for offline database status
            BlittableJsonReaderObject settings;
            if (data.TryGet("Settings", out settings))
            {
                bool indexingDisable;
                if (settings.TryGet(RavenConfiguration.GetKey(x => x.Indexing.Disabled), out indexingDisable) &&
                    indexingDisable)
                    indexingStatus = "Disabled";
            }

            if (online == false)
            {
                // if state of database is found in the cache we can continue
                if (ServerStore.DatabaseInfoCache.TryWriteOfflineDatabaseStatustoRequest(
                    context, writer, databaseName, disabled, indexingStatus))
                {
                    return;
                }
                // we won't find it if it is a new database or after a dirty shutdown, so just report empty values then
            }

            var size = new Size(GetTotalSize(db));
            var backupInfo = GetBackupInfo(db);

            var doc = new DynamicJsonValue
            {
                [nameof(DatabaseInfo.Bundles)] = new DynamicJsonArray(GetBundles(db)),
                [nameof(DatabaseInfo.IsAdmin)] = true, //TODO: implement me!
                [nameof(DatabaseInfo.Name)] = databaseName,
                [nameof(DatabaseInfo.Disabled)] = disabled,
                [nameof(DatabaseInfo.TotalSize)] = new DynamicJsonValue
                {
                    [nameof(Size.HumaneSize)] = size.HumaneSize,
                    [nameof(Size.SizeInBytes)] = size.SizeInBytes
                },
                [nameof(DatabaseInfo.Errors)] = online
                    ? db.IndexStore.GetIndexes().Sum(index => index.GetErrors().Count)
                    : 0,
                [nameof(DatabaseInfo.Alerts)] = online ? db.NotificationCenter.GetAlertCount() : 0,
                [nameof(DatabaseInfo.UpTime)] = online ? GetUptime(db).ToString() : null,
                [nameof(DatabaseInfo.BackupInfo)] = backupInfo,
                [nameof(DatabaseInfo.DocumentsCount)] = online
                    ? db.DocumentsStorage.GetNumberOfDocuments()
                    : 0,
                [nameof(DatabaseInfo.IndexesCount)] = online ? db.IndexStore.GetIndexes().Count() : 0,
                [nameof(DatabaseInfo.RejectClients)] = false, //TODO: implement me!
                [nameof(DatabaseInfo.IndexingStatus)] = indexingStatus
            };

            context.Write(writer, doc);
        }

        private void WriteFaultedDatabaseInfo(TransactionOperationContext context, BlittableJsonTextWriter writer, Task<DocumentDatabase> dbTask, string databaseName)
        {
            var exception = dbTask.Exception;

            var doc = new DynamicJsonValue
            {
                [nameof(DatabaseInfo.Name)] = databaseName,
                [nameof(DatabaseInfo.LoadError)] = exception.ExtractSingleInnerException().Message
            };

            context.Write(writer, doc);
        }

        private DynamicJsonValue GetBackupInfo(DocumentDatabase db)
        {
            var periodicExportRunner = db?.BundleLoader.PeriodicExportRunner;

            if (periodicExportRunner == null)
            {
                return null;
            }

            return new DynamicJsonValue
            {
                [nameof(BackupInfo.IncrementalBackupInterval)] = periodicExportRunner.IncrementalInterval,
                [nameof(BackupInfo.FullBackupInterval)] = periodicExportRunner.FullExportInterval,
                [nameof(BackupInfo.LastIncrementalBackup)] = periodicExportRunner.ExportTime,
                [nameof(BackupInfo.LastFullBackup)] = periodicExportRunner.FullExportTime
            };
        }

        private TimeSpan GetUptime(DocumentDatabase db)
        {
            return SystemTime.UtcNow - db.StartTime;
        }

        private long GetTotalSize(DocumentDatabase db)
        {
            if (db == null)
                return 0;

            return
                db.GetAllStoragesEnvironment()
                    .Sum(env => env.Environment.Stats().AllocatedDataFileSizeInBytes);
        }

        private List<string> GetBundles(DocumentDatabase db)
        {
            if (db != null)
                return db.BundleLoader.GetActiveBundles();
            return new List<string>();
        }
    }
}

