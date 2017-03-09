//-----------------------------------------------------------------------
// <copyright file="ISyncAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Client.Documents.Commands;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced synchronous session operations
    /// </summary>
    public partial interface IAdvancedSessionOperation
    {
        /// <summary>
        ///     Stream the results on the query to the client, converting them to
        ///     CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="query">Query to stream results for</param>
        IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query);

        /// <summary>
        ///     Stream the results on the query to the client, converting them to
        ///     CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="query">Query to stream results for</param>
        IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query);

        /// <summary>
        ///     Stream the results of documents search to the client, converting them to CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="fromEtag">ETag of a document from which stream should start</param>
        /// <param name="start">number of documents that should be skipped</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved</param>
        /// <param name="transformer">name of a transformer that should be used to transform the results</param>
        /// <param name="transformerParameters">parameters that will be passed to transformer</param>
        IEnumerator<StreamResult<T>> Stream<T>(long? fromEtag, int start = 0, int pageSize = int.MaxValue, string transformer = null, Dictionary<string, object> transformerParameters = null);

        /// <summary>
        ///     Stream the results of documents search to the client, converting them to CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="startsWith">prefix for which documents should be returned e.g. "products/"</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved</param>
        /// <param name="startAfter">
        ///     skip document fetching until given key is found and return documents after that key (default:
        ///     null)
        /// </param>
        /// <param name="transformer">name of a transformer that should be used to transform the results</param>
        /// <param name="transformerParameters">parameters that will be passed to transformer</param>
        IEnumerator<StreamResult<T>> Stream<T>(string startsWith, string matches = null, int start = 0, int pageSize = int.MaxValue, string startAfter = null, string transformer = null, Dictionary<string, object> transformerParameters = null);

        /// <summary>
        ///     Returns the results of a query directly into stream 
        /// </summary>
        void StreamInto<T>(IDocumentQuery<T> query, Stream output);

    }
}
