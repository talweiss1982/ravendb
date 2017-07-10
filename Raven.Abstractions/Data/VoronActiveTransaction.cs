// -----------------------------------------------------------------------
//  <copyright file="VoronActiveTransaction.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Abstractions.Data
{
    public class VoronActiveTransaction
    {
        public long Id;
        public string Flags;
        public string TransactionStackTrace { get; set; }
    }
}
