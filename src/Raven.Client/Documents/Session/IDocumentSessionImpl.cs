//-----------------------------------------------------------------------
// <copyright file="IDocumentSessionImpl.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.Operations.Lazy;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Interface for document session which holds the internal operations
    /// </summary>
    internal interface IDocumentSessionImpl : IDocumentSession, ILazySessionOperations, IEagerSessionOperations
    {
        DocumentConventions Conventions { get; }
        Dictionary<string, T> LoadInternal<T>(string[] ids, string[] includes);
        Dictionary<string, T> LoadInternal<T>(string[] ids, string[] includes, string transformer, Dictionary<string, object> transformerParameters = null);
        Lazy<Dictionary<string, T>> LazyLoadInternal<T>(string[] ids, string[] includes, Action<Dictionary<string, T>> onEval);
    }
}
