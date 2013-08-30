using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Linq;
using Raven.Abstractions.Logging;
using Raven.Database.Indexing;
using Raven.Database.Linq;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Voron;
using Voron.Impl;
using Voron.Trees;

namespace Raven.AggregationEngine
{
	public class Aggregator : IDisposable
	{
		private ILog Log = LogManager.GetCurrentClassLogger();

		private readonly AggregationEngine _aggregationEngine;
		private readonly string _name;
		private readonly AbstractViewGenerator _generator;
		private long _lastAggregatedEtag;
		private readonly LruCache<string, RavenJToken> _cache = new LruCache<string, RavenJToken>(2048, StringComparer.InvariantCultureIgnoreCase);
		private Task _aggregationTask;
		private readonly AsyncEvent _aggregationCompleted = new AsyncEvent();
		private readonly Reference<int> _appendEventState = new Reference<int>();
		private readonly TaskCompletionSource<object> _disposedCompletionSource = new TaskCompletionSource<object>();
		private volatile bool _disposed;
		private AggregateException _aggregateException;
		private readonly Slice _nameKey;


		public Aggregator(Transaction tx, AggregationEngine aggregationEngine, string name, AbstractViewGenerator generator)
		{
			_aggregationEngine = aggregationEngine;
			_name = name;
			_nameKey = name;
			_generator = generator;

			var aggregationStatus = aggregationEngine.Storage.GetTree(tx, AggregationEngine.AggregationStatusKey);
			using (var result = aggregationStatus.Read(tx, _nameKey))
			{
				if (result == null)
				{
					_lastAggregatedEtag = 0;
					return;
				}
				using (var br = new BinaryReader(result.Stream))
				{
					_lastAggregatedEtag = br.ReadInt64();
				}
			}
		}

		public AbstractViewGenerator Generator
		{
			get { return _generator; }
		}

		public void StartAggregation()
		{
			_aggregationTask = AggregateAsync()
				.ContinueWith(task =>
					{
						if (!task.IsFaulted)
							return;
						Log.ErrorException("An error occured when running background aggregation for " + _name + ", the aggregation has been DISABLED", task.Exception);
						_aggregateException = task.Exception;
					});
		}

		private async Task AggregateAsync()
		{
			var lastAggregatedEtag = _lastAggregatedEtag;
			while (_aggregationEngine.CancellationToken.IsCancellationRequested == false)
			{
				var eventDatas = _aggregationEngine.Events(lastAggregatedEtag).Take(1024)
					.ToArray();
				if (eventDatas.Length == 0)
				{
					await _aggregationEngine.WaitForAppendAsync(_appendEventState);
					continue;
				}
				try
				{
					var items = eventDatas.Select(x => new DynamicJsonObject(x.Data)).ToArray();
					var writeBatch = new WriteBatch();
					var groupedByReduceKey = ExecuteMaps(items);
					ExecuteReduce(groupedByReduceKey, writeBatch);
					lastAggregatedEtag = eventDatas.Last().Key;
					writeBatch.Add(_nameKey, GetStreamForInt64(lastAggregatedEtag), AggregationEngine.AggregationStatusKey);
					await _aggregationEngine.Storage.Writer.WriteAsync(writeBatch);
				}
				catch (Exception e)
				{
					Log.ErrorException("Could not process aggregation", e);
					throw;
				}
				finally
				{
					Thread.VolatileWrite(ref _lastAggregatedEtag, lastAggregatedEtag);

					_aggregationCompleted.PulseAll();
				}
			}
		}

		private static Stream GetStreamForInt64(long l)
		{
			var ms = new MemoryStream(8);
			var bw = new BinaryWriter(ms);
			bw.Write(l);
			ms.Position = 0;
			return ms;
		}

		private void ExecuteReduce(IEnumerable<IGrouping<dynamic, object>> groupedByReduceKey, WriteBatch writeBatch)
		{
			using (var sp = _aggregationEngine.Storage.CreateSnapshot())
			{
				foreach (var grouping in groupedByReduceKey)
				{
					string reduceKey = grouping.Key;
					Slice key = reduceKey;
					var groupedResults = GetItemsToReduce(sp, reduceKey, key, grouping);

					var robustEnumerator = new RobustEnumerator(_aggregationEngine.CancellationToken, 50)
					{
						OnError = (exception, o) =>
								  Log.WarnException("Could not process reduce for aggregation " + _name + Environment.NewLine + o,
													exception)
					};

					var reduceResults =
						robustEnumerator.RobustEnumeration(groupedResults.GetEnumerator(), _generator.ReduceDefinition).ToArray();

					RavenJToken finalResult;
					switch (reduceResults.Length)
					{
						case 0:
							Log.Warn("FLYING PIGS!!! Could not find any results for a reduce on key {0} for aggregator {1}. Should not happen", reduceKey, _name);
							finalResult = new RavenJObject { { "Error", "Invalid reduce result was generated" } };
							break;
						case 1:
							finalResult = RavenJObject.FromObject(reduceResults[0]);
							break;
						default:
							finalResult = new RavenJArray(reduceResults.Select(RavenJObject.FromObject));
							break;
					}
					finalResult.EnsureCannotBeChangeAndEnableSnapshotting();
					_cache.Set(reduceKey, finalResult);
					writeBatch.Add(key, AggregationEngine.RavenJTokenToStream(finalResult), _name);
				}
			}
		}

		private IEnumerable<dynamic> GetItemsToReduce(SnapshotReader sp, dynamic reduceKey, Slice key, IGrouping<dynamic, object> grouping)
		{
			RavenJToken currentStatus;
			if (_cache.TryGet(reduceKey, out currentStatus) == false)
			{
				using (var result = sp.Read(_name, key))
				{
					if (result != null)
					{
						currentStatus = RavenJToken.ReadFrom(new JsonTextReader(new StreamReader(result.Stream)));
					}
				}
			}

			if (currentStatus == null)
			{
				return grouping;
			}
			IEnumerable<dynamic> groupedResults = grouping;
			switch (currentStatus.Type)
			{
				case JTokenType.Array:
					groupedResults =
						groupedResults.Concat(((RavenJArray)currentStatus).Select(x => new DynamicJsonObject((RavenJObject)x)));
					break;
				case JTokenType.Object:
					groupedResults = grouping.Concat(new dynamic[] { new DynamicJsonObject((RavenJObject)currentStatus) });
					break;
			}
			return groupedResults;
		}

		private IEnumerable<IGrouping<dynamic, object>> ExecuteMaps(IEnumerable<object> items)
		{
			var robustEnumerator = new RobustEnumerator(_aggregationEngine.CancellationToken, 50)
				{
					OnError = (exception, o) =>
						Log.WarnException("Could not process maps event for aggregator: " + _name + Environment.NewLine + o, exception)
				};

			var results = robustEnumerator
				.RobustEnumeration(items.GetEnumerator(), _generator.MapDefinitions)
				.ToList();

			var reduced = robustEnumerator.RobustEnumeration(results.GetEnumerator(), _generator.ReduceDefinition);

			var groupedByReduceKey = reduced.GroupBy(x =>
				{
					var reduceKey = _generator.GroupByExtraction(x);
					if (reduceKey == null)
						return "@null";
					var s = reduceKey as string;
					if (s != null)
						return s;
					var ravenJToken = RavenJToken.FromObject(reduceKey);
					if (ravenJToken.Type == JTokenType.String)
						return ravenJToken.Value<string>();
					return ravenJToken.ToString(Formatting.None);
				})
											.ToArray();
			return groupedByReduceKey;
		}

		public void Dispose()
		{
			if (_disposed)
				return;
			ThreadPool.QueueUserWorkItem(state => DisposeAsync());
		}

		public async Task WaitForEtagAsync(long id)
		{
			if (id == 0)
				return;
			var callerState = new Reference<int>();
			while (true)
			{
				var lastAggregatedEtag = LastAggregatedEtag;
				if (id <= lastAggregatedEtag)
					return;
				await _aggregationCompleted.WaitAsync(callerState);
			}
		}

		public RavenJToken AggregationResultFor(string item)
		{
			if (_aggregateException != null)
				throw new AggregateException(_aggregateException.InnerExceptions);

			RavenJToken value;
			if (_cache.TryGet(item, out value))
				return value.CreateSnapshot();
			Slice key = item;
			using (var sp = _aggregationEngine.Storage.CreateSnapshot())
			using (var result = sp.Read(_name, key))
			{
				if (result != null)
				{
					value = RavenJToken.ReadFrom(new JsonTextReader(new StreamReader(result.Stream)));
					value.EnsureCannotBeChangeAndEnableSnapshotting();
					_cache.Set(item, value);
					return value.CreateSnapshot();
				}
				return null;
			}
		}

		public long LastAggregatedEtag
		{
			get { return Thread.VolatileRead(ref _lastAggregatedEtag); }
		}

		public async Task DisposeAsync()
		{
			if (_disposed)
				return;
			_disposed = true;
			_disposedCompletionSource.SetResult(null);
			_aggregationCompleted.Dispose();
			if (_aggregationTask != null)
				await _aggregationTask;
		}

		public IEnumerable<ReductionData> AggregationResults()
		{
			using(var sp = _aggregationEngine.Storage.CreateSnapshot())
			using (var it = sp.Iterate(_name))
			{
				if (it.Seek(Slice.BeforeAllKeys) == false)
					yield break;
				do
				{
					using (var stream = it.CreateStreamForCurrent())
					{
						var ravenJToken = RavenJToken.ReadFrom(new JsonTextReader(new StreamReader(stream)));
						yield return new ReductionData
						{
							Data = ravenJToken,
							ReduceKey = it.CurrentKey.ToString()
						};
					}
				} while (it.MoveNext());
			}
		}
	}
}