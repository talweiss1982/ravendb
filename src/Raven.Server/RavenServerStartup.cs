using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using AsyncFriendlyStackTrace;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Exceptions;
using Raven.Client.Documents.Exceptions.Compilation;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Compilation;
using Raven.Client.Exceptions.Database;
using Raven.Server.Config.Attributes;
using Raven.Server.Routing;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using ConcurrencyException = Voron.Exceptions.ConcurrencyException;

namespace Raven.Server
{
    public class RavenServerStartup
    {
        private RequestRouter _router;
        private RavenServer _server;
        private int _requestId;
        private readonly Logger _logger = LoggingSource.Instance.GetLogger<RavenServerStartup>("Server");

        public void Configure(IApplicationBuilder app, ILoggerFactory loggerfactory)
        {
            app.UseWebSockets(new WebSocketOptions
            {
                // TODO: KeepAlive causes "Unexpect reserved bit set" (we are sending our own hearbeats, so we do not need this)
                //KeepAliveInterval = Debugger.IsAttached ? 
                //    TimeSpan.FromHours(24) : TimeSpan.FromSeconds(30), 
                KeepAliveInterval = TimeSpan.FromHours(24),
                ReceiveBufferSize = 4096,
            });

            _router = app.ApplicationServices.GetService<RequestRouter>();
            _server = app.ApplicationServices.GetService<RavenServer>();
            if (IsAccessUnauthorized())
            {
                app.Run(UnAuthorizedRequestHandler);
            }
            app.Run(RequestHandler);
        }

        private bool IsAccessUnauthorized()
        {
            if (_server.Configuration.Server.AnonymousUserAccessMode == AnonymousUserAccessModeValues.None)
                return false;
            if (_server.Configuration.Server.AllowAdminAnonymousAccessWhenNotbindedToLocalhost == true)
                return false;
            var url = _server.Configuration.Core.ServerUrl.ToLowerInvariant();
            var uri = new Uri(url);
            //url isn't set to localhost 
            return !(uri.IsLoopback  || uri.Host == "0.0.0.0");
        }

        public static bool SkipHttpLogging;

        private Task UnAuthorizedRequestHandler(HttpContext context)
        {
            context.Response.StatusCode = (int) HttpStatusCode.ServiceUnavailable;
            context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
            JsonOperationContext ctx;
            using (_server.ServerStore.ContextPool.AllocateOperationContext(out ctx))
            using (var writer = new BlittableJsonTextWriter(ctx, context.Response.Body))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Message");
                writer.WriteString($"The server is running in an un-authorized mode.{Environment.NewLine}" +
                                   $"This means that Raven/AnonymousUserAccessMode is set to Admin and expose to the world.{Environment.NewLine}" +
                                   $"Either set Raven/AnonymousUserAccessMode to None or set Raven/AllowAdminAnonymousAccessWhenNotbindedToLocalhost to true.{Environment.NewLine}" +
                                   $"In order to gain access to the server please run in on localhost.{Environment.NewLine}");
                writer.WriteEndObject();
            }
            return Task.CompletedTask;
        }

        private async Task RequestHandler(HttpContext context)
        {
            try
            {                
                context.Response.StatusCode = (int)HttpStatusCode.OK;
                context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";

                var sp = Stopwatch.StartNew();
                var tenant = await _router.HandlePath(context, context.Request.Method, context.Request.Path.Value);
                sp.Stop();

                if (_logger.IsInfoEnabled && SkipHttpLogging == false)
                {
                    _logger.Info($"{context.Request.Method} {context.Request.Path.Value}?{context.Request.QueryString.Value} - {context.Response.StatusCode} - {sp.ElapsedMilliseconds:#,#;;0} ms");
                }

                if (TrafficWatchManager.HasRegisteredClients)
                {
                    var requestId = Interlocked.Increment(ref _requestId);

                    var twn = new TrafficWatchChange
                    {
                        TimeStamp = DateTime.UtcNow,
                        RequestId = requestId, // counted only for traffic watch
                        HttpMethod = context.Request.Method ?? "N/A", // N/A ?
                        ElapsedMilliseconds = sp.ElapsedMilliseconds,
                        ResponseStatusCode = context.Response.StatusCode,
                        RequestUri = context.Request.GetEncodedUrl(),
                        AbsoluteUri = $@"{context.Request.Scheme}://{context.Request.Host}",
                        TenantName = tenant ?? "N/A",
                        CustomInfo = "", // TODO: Implement
                        InnerRequestsCount = 0, // TODO: Implement
                        QueryTimings = null, // TODO: Implement
                    };

                    TrafficWatchManager.DispatchMessage(twn);
                }
            }
            catch (Exception e)
            {
                if (context.RequestAborted.IsCancellationRequested)
                    return;

                //TODO: special handling for argument exception (400 bad request)
                //TODO: operaton cancelled (timeout)
                //TODO: Invalid data exception 422


                //TODO: Proper json output, not like this
                var response = context.Response;

                MaybeSetExceptionStatusCode(response, e);

                JsonOperationContext ctx;
                using (_server.ServerStore.ContextPool.AllocateOperationContext(out ctx))
                {
                    var djv = new DynamicJsonValue
                    {
                        [nameof(ExceptionDispatcher.ExceptionSchema.Url)] = $"{context.Request.Path}?{context.Request.QueryString}",
                        [nameof(ExceptionDispatcher.ExceptionSchema.Type)] = e.GetType().FullName,
                        [nameof(ExceptionDispatcher.ExceptionSchema.Message)] = e.Message
                    };

                    string errorString;

                    try
                    {
                        errorString = e.ToAsyncString();
                    }
                    catch (Exception)
                    {
                        errorString = e.ToString();
                    }

                    djv[nameof(ExceptionDispatcher.ExceptionSchema.Error)] = errorString;

                    MaybeAddAdditionalExceptionData(djv, e);

                    using (var writer = new BlittableJsonTextWriter(ctx, response.Body))
                    {
                        var json = ctx.ReadObject(djv, "exception");
                        writer.WriteObject(json);
                    }
                }
            }
        }

        private void MaybeAddAdditionalExceptionData(DynamicJsonValue djv, Exception exception)
        {
            var indexCompilationException = exception as IndexCompilationException;
            if (indexCompilationException != null)
            {
                djv[nameof(IndexCompilationException.IndexDefinitionProperty)] = indexCompilationException.IndexDefinitionProperty;
                djv[nameof(IndexCompilationException.ProblematicText)] = indexCompilationException.ProblematicText;
                return;
            }

            var transformerCompilationException = exception as TransformerCompilationException;
            if (transformerCompilationException != null)
            {
                djv[nameof(TransformerCompilationException.TransformerDefinitionProperty)] = transformerCompilationException.TransformerDefinitionProperty;
                djv[nameof(TransformerCompilationException.ProblematicText)] = transformerCompilationException.ProblematicText;
                return;
            }

            var documentConflictException = exception as DocumentConflictException;
            if (documentConflictException != null)
            {
                djv[nameof(DocumentConflictException.DocId)] = documentConflictException.DocId;
                djv[nameof(DocumentConflictException.Conflicts)] = documentConflictException.GetConflicts();
                return;
            }
        }

        private static void MaybeSetExceptionStatusCode(HttpResponse response, Exception exception)
        {
            if (response.HasStarted)
                return;

            if (exception is DocumentConflictException)
            {
                response.StatusCode = (int)HttpStatusCode.Conflict;
                return;
            }

            if (exception is ConflictException)
            {
                response.StatusCode = (int)HttpStatusCode.Conflict;
                return;
            }

            if (exception is ConcurrencyException)
            {
                response.StatusCode = (int)HttpStatusCode.Conflict;
                return;
            }

            if (exception is DatabaseNotFoundException || exception is DatabaseDisabledException)
            {
                response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
                return;
            }

            response.StatusCode = (int)HttpStatusCode.InternalServerError;
        }
    }
}
