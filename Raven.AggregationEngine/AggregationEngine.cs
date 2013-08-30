using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Database.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using System.Linq;
using Voron;
using Voron.Impl;
using Voron.Trees;

namespace Raven.AggregationEngine
{
	public class AggregationEngine : IDisposable
	{
		private static readonly ILog _log = LogManager.GetCurrentClassLogger();
		private readonly ConcurrentDictionary<string, Aggregator> _aggregationInstances
			= new ConcurrentDictionary<string, Aggregator>(StringComparer.InvariantCultureIgnoreCase);

		private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
		private readonly string _path;

		private long _lastKey;

		private readonly AsyncEvent _appendEvent = new AsyncEvent();
		private readonly Tree _tags;
		private readonly Tree _events;
		private readonly Tree _aggregations;

		public AggregationEngine(string path = null, IVirtualPager pager = null)
		{
			_path = path ?? Path.GetTempPath();
			Pager = pager ?? (path == null ? (IVirtualPager)new PureMemoryPager() : new MemoryMapPager(path));
			Storage = new StorageEnvironment(Pager, pager == null);

			using (var tx = Storage.NewTransaction(TransactionFlags.ReadWrite))
			{
				_events = Storage.CreateTree(tx, EventsKey);
				_tags = Storage.CreateTree(tx, TagsKey);
				_aggregations = Storage.CreateTree(tx, AggregationKey);

				Storage.CreateTree(tx, AggregationStatusKey);

				_lastKey = ReadLastKey(tx);
				ReadAllAggregations(tx);

				tx.Commit();
			}
		}

		public IVirtualPager Pager { get; private set; }

		public StorageEnvironment Storage { get; private set; }

		private long ReadLastKey(Transaction tx)
		{
			using (var it = _events.Iterate(tx))
			{
				if (it.Seek(Slice.AfterAllKeys) == false)
					return 0;
				return it.CurrentKey.ToInt64();
			}
		}

		private void ReadAllAggregations(Transaction tx)
		{
			using (var it = _aggregations.Iterate(tx))
			{
				if (it.Seek(Slice.BeforeAllKeys) == false)
					return;
				do
				{
					using (var stream = it.CreateStreamForCurrent())
					{
						var indexDef = new StreamReader(stream).ReadToEnd();
						var indexDefinition =
							new JsonSerializer().Deserialize<IndexDefinition>(new JsonTextReader(new StringReader(indexDef)));
						_log.Info("Reading aggregator {0}", indexDefinition.Name);
						AbstractViewGenerator generator;
						try
						{
							var dynamicViewCompiler = new DynamicViewCompiler(indexDefinition.Name, indexDefinition,
																			  Path.Combine(_path, "Generators"));
							generator = dynamicViewCompiler.GenerateInstance();
						}
						catch (Exception e)
						{
							_log.WarnException("Could not create instance of aggregator " + indexDefinition.Name +
											   Environment.NewLine + indexDef, e);
							// could not create generator, ignoring this and deleting the generator

							Storage.DeleteTree(tx, it.CurrentKey.ToString());
							_aggregations.Delete(tx, it.CurrentKey);
							it.MovePrev();// compensate for delete
							continue;
						}
						Storage.CreateTree(tx, indexDefinition.Name);
						var aggregator = new Aggregator(tx, this, indexDefinition.Name, generator);
						_aggregationInstances.TryAdd(indexDefinition.Name, aggregator);
						Background.Work(aggregator.StartAggregation);
					}
				} while (it.MoveNext());
			}
		}

		public const string EventsKey = "events";
		public const string TagsKey = "tags";
		public const string AggregationKey = "aggregations";
		public const string AggregationStatusKey = "aggregation-status";

		public void Dispose()
		{
			if (_cancellationTokenSource.IsCancellationRequested)
				return;
			ThreadPool.QueueUserWorkItem(_ => DisposeAsync());
		}

		public void CreateAggregation(IndexDefinition indexDefinition)
		{
			var dynamicViewCompiler = new DynamicViewCompiler(indexDefinition.Name, indexDefinition, Path.Combine(_path, "Generators"));
			var generator = dynamicViewCompiler.GenerateInstance();

			Aggregator aggregator;
			using (var tx = Storage.NewTransaction(TransactionFlags.ReadWrite))
			{
				_aggregations.Add(tx, indexDefinition.Name, SmallObjectToMemoryStream(indexDefinition));
				Storage.CreateTree(tx, indexDefinition.Name);

				aggregator = new Aggregator(tx, this, indexDefinition.Name, generator);

				tx.Commit();
			}

			_aggregationInstances.AddOrUpdate(indexDefinition.Name, aggregator, (s, viewGenerator) => aggregator);
			Background.Work(aggregator.StartAggregation);
		}

		private static MemoryStream SmallObjectToMemoryStream(object obj)
		{
			return RavenJTokenToStream(RavenJObject.FromObject(obj));
		}

		internal static MemoryStream RavenJTokenToStream(RavenJToken item)
		{
			var memoryStream = new MemoryStream();
			var streamWriter = new StreamWriter(memoryStream);
			item.WriteTo(new JsonTextWriter(streamWriter));
			streamWriter.Flush();
			memoryStream.Position = 0;
			return memoryStream;
		}

		public Aggregator GetAggregation(string name)
		{
			Aggregator value;
			_aggregationInstances.TryGetValue(name, out value);
			return value;
		}

		public Task<long> AppendAsync(string topic, string[] tags, params RavenJObject[] items)
		{
			return AppendAsync(topic, tags, (IEnumerable<RavenJObject>)items);
		}

		public async Task<long> AppendAsync(string topic, string[] tags, IEnumerable<RavenJObject> items)
		{
			if (items == null) throw new ArgumentNullException("items");

			var batch = new WriteBatch();

			var tagsAsKeys = tags.Select(s => (Slice)s).ToArray();


			long key = 0;
			foreach (var item in items)
			{
				RavenJToken metadata;
				if (item.TryGetValue("@metadata", out metadata) == false)
					item["@metadata"] = metadata = new RavenJObject();
				((RavenJObject)metadata)["Raven-Entity-Name"] = topic;
				key = Interlocked.Increment(ref _lastKey);
				var eventKey = new Slice(BitConverter.GetBytes(key));
				batch.Add(eventKey, RavenJTokenToStream(item), EventsKey);
				foreach (var tag in tagsAsKeys)
				{
					batch.MultiAdd(tag, eventKey, TagsKey);
				}
			}

			await Storage.Writer.WriteAsync(batch);
			_appendEvent.PulseAll();
			return key;
		}

		public IEnumerable<EventData> Events(long after)
		{
			using (var snapshot = Storage.CreateSnapshot())
			using (var it = snapshot.Iterate(EventsKey))
			{
				var key = new Slice(BitConverter.GetBytes(after + 1));
				if (it.Seek(key) == false)
					yield break;
				do
				{
					using (var stream = it.CreateStreamForCurrent())
					{
						var token = RavenJToken.ReadFrom(new JsonTextReader(new StreamReader(stream)));
						yield return new EventData
						{
							Data = (RavenJObject)token,
							Key = it.CurrentKey.ToInt64()
						};
					}

				} while (it.MoveNext());
			}
		}

		public IEnumerable<EventData> EventsByTag(string tag, long after)
		{
			using (var snapshot = Storage.CreateSnapshot())
			using (var it = snapshot.MultiRead(TagsKey, tag))
			{
				var key = new Slice(BitConverter.GetBytes(after + 1));
				if (it.Seek(key) == false)
					yield break;
				do
				{

					using (var readResult = snapshot.Read(EventsKey, it.CurrentKey))
					{
						if (readResult == null)
							throw new InvalidOperationException("Probable Bug/Corruption: Missing event from tag stream: " + tag);

						var token = RavenJToken.ReadFrom(new JsonTextReader(new StreamReader(readResult.Stream)));
						yield return new EventData
						{
							Data = (RavenJObject)token,
							Key = it.CurrentKey.ToInt64()
						};
					}
				} while (it.MoveNext());
			}
		}

		public Task<bool> WaitForAppendAsync(Reference<int> callerState)
		{
			return _appendEvent.WaitAsync(callerState);
		}

		public CancellationToken CancellationToken
		{
			get { return _cancellationTokenSource.Token; }
		}
		public long LastKey
		{
			get { return _lastKey; }
		}

		public async Task DisposeAsync()
		{
			if (_cancellationTokenSource.IsCancellationRequested)
				return;

			_cancellationTokenSource.Cancel();

			_appendEvent.Dispose();

			await Task.WhenAll(_aggregationInstances.Values.Select(aggregator => aggregator.DisposeAsync()));

			Storage.Dispose();
		}
	}
}