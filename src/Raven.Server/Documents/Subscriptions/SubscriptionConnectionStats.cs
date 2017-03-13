﻿using System;
using Raven.Server.Utils.Metrics;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionConnectionStats:IDisposable
    {
        public SubscriptionConnectionStats()
        {
            DocsRate = new MeterMetric();
            BytesRate = new MeterMetric();
            AckRate = new MeterMetric();
        }
        
        public DateTime ConnectedAt;

        public DateTime LastMessageSentAt;
        
        public DateTime LastSentEtag;

        public DateTime LastAckReceivedAt;
        public DateTime LastAckedEtag;

        internal readonly MeterMetric DocsRate;
        public MeterMetric BytesRate;
        public MeterMetric AckRate;

        public void Dispose()
        {
            DocsRate?.Dispose();
            BytesRate?.Dispose();
            AckRate?.Dispose();
        }
    }
}