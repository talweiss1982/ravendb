using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Json.Converters;
using Raven.Server.Commercial;
using Raven.Server.Documents.Patch;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class TimeSeriesHandler : DatabaseRequestHandler
    {

        [RavenAction("/databases/*/timeseries", "GET", AuthorizationStatus.ValidUser)]
        public Task Read()
        {
            var documentId = GetStringQueryString("id");
            var name = GetStringQueryString("name");
            var from = GetDateTimeQueryString("from", required: false) ?? DateTime.MinValue;
            var to = GetDateTimeQueryString("to", required: false) ?? DateTime.MaxValue;


            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var reader = Database.DocumentsStorage.TimeSeriesStorage.GetReader(context, documentId, name, from, to);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    {

                        writer.WritePropertyName("Id");
                        writer.WriteString(documentId);
                        writer.WriteComma();

                        writer.WritePropertyName("Values");
                        writer.WriteStartObject();
                        {

                            writer.WritePropertyName(name);

                            writer.WriteStartObject();
                            {

                                writer.WritePropertyName("Name");
                                writer.WriteString(name);
                                writer.WriteComma();

                                writer.WritePropertyName("From");
                                writer.WriteDateTime(from, true);
                                writer.WriteComma();


                                writer.WritePropertyName("To");
                                writer.WriteDateTime(to, true);
                                writer.WriteComma();

                                writer.WritePropertyName("FullRange");
                                writer.WriteBool(false); // TODO: Need to figure this out
                                writer.WriteComma();

                                writer.WritePropertyName("Values");
                                writer.WriteStartArray();
                                {
                                    var first = true;
                                    foreach (var item in reader.Values())
                                    {
                                        if (first)
                                        {
                                            first = false;
                                        }
                                        else
                                        {
                                            writer.WriteComma();
                                        }
                                        writer.WriteStartObject();

                                        writer.WritePropertyName("Timestamp");
                                        writer.WriteDateTime(item.TimeStamp, true);
                                        writer.WriteComma();
                                        writer.WritePropertyName("Tag");
                                        writer.WriteString(item.Tag);
                                        writer.WriteComma();
                                        writer.WriteArray("Values", item.Values);

                                        writer.WriteEndObject();
                                    }
                                }
                                writer.WriteEndArray();


                            }
                            writer.WriteEndObject();

                        }
                        writer.WriteEndObject();

                    }
                    writer.WriteEndObject();

                    writer.Flush();
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/timeseries", "POST", AuthorizationStatus.ValidUser)]
        public async Task Batch()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var blittable = await context.ReadForMemoryAsync(RequestBodyStream(), "timeseries");

                var timeSeriesBatch = JsonDeserializationClient.DocumentTimeSeriesOperation(blittable);

                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(blittable.ToString(), TrafficWatchChangeType.TimeSeries);

                var cmd = new ExecuteTimeSeriesBatchCommand(Database, timeSeriesBatch, false);

                try
                {
                    await Database.TxMerger.Enqueue(cmd);
                }
                catch (DocumentDoesNotExistException)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    throw;
                }
            }
        }


        public class ExecuteTimeSeriesBatchCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly DocumentDatabase _database;
            private readonly DocumentTimeSeriesOperation _batch;
            private readonly bool _fromEtl;

            public string LastChangeVector;

            public ExecuteTimeSeriesBatchCommand(DocumentDatabase database, DocumentTimeSeriesOperation batch, bool fromEtl)
            {
                _database = database;
                _batch = batch;
                _fromEtl = fromEtl;
            }

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                int changes = 0;
                string docCollection = GetDocumentCollection(context, _batch);

                if (docCollection == null)
                    return 0;

                var tss = _database.DocumentsStorage.TimeSeriesStorage;

                if (_batch.Appends != null)
                {
                    foreach (var append in _batch.Appends)
                    {
                        LastChangeVector = tss.AppendTimestamp(context,
                            _batch.Id,
                            docCollection,
                            append.Name,
                            append.Timestamp,
                            new Span<double>(append.Values),
                            append.Tag,
                            fromReplication: false
                        );

                        changes++;
                    }
                }

                if (_batch.Removals != null)
                {
                    foreach (var removal in _batch.Removals)
                    {
                        LastChangeVector = tss.RemoveTimestampRange(context,
                            _batch.Id,
                            docCollection,
                            removal.Name,
                            removal.From,
                            removal.To
                        );
                        changes++;
                    }
                }
                return changes;
            }

            private string GetDocumentCollection(DocumentsOperationContext context, DocumentTimeSeriesOperation docBatch)
            {
                try
                {
                    var doc = _database.DocumentsStorage.Get(context, docBatch.Id,
                         throwOnConflict: true);
                    if (doc == null)
                    {
                        if (_fromEtl)
                            return null;

                        ThrowMissingDocument(docBatch.Id);
                        return null;// never hit
                    }

                    if (doc.Flags.HasFlag(DocumentFlags.Artificial))
                        ThrowArtificialDocument(doc);

                    return CollectionName.GetCollectionName(doc.Data);
                }
                catch (DocumentConflictException)
                {
                    if (_fromEtl)
                        return null;

                    // this is fine, we explicitly support
                    // setting the flag if we are in conflicted state is 
                    // done by the conflict resolver

                    // avoid loading same document again, we validate write using the metadata instance
                    return _database.DocumentsStorage.ConflictsStorage.GetCollection(context, docBatch.Id);
                }
            }

            private static void ThrowMissingDocument(string docId)
            {
                throw new DocumentDoesNotExistException(docId, "Cannot operate on time series of a missing document");
            }


            public static void ThrowArtificialDocument(Document doc)
            {
                throw new InvalidOperationException($"Document '{doc.Id}' has '{nameof(DocumentFlags.Artificial)}' flag set. " +
                                                    "Cannot put TimeSeries on artificial documents.");
            }


            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                throw new System.NotImplementedException();
            }
        }

    }
}