﻿using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public interface ICommandData
    {
        string Id { get; }

        string Name { get; }

        string ChangeVector { get; }

        CommandType Type { get; }

        DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context);
    }

    public enum CommandType
    {
        None,
        PUT,
        PATCH,
        DELETE,
        AttachmentPUT,
        AttachmentDELETE,

        // NOTE: When you add anything here, such as additional 
        // operation relating to a document but not operating on it 
        // directly, be sure to also update DeferInternal
        // to recognize that these are allows, like with AttachmentPUT and
        // AttachmentDELETE

        ClientAnyCommand,
        ClientNotAttachment
    }
}
