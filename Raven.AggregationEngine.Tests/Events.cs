using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Json.Linq;
using Voron.Impl;
using Xunit;

namespace Raven.AggregationEngine.Tests
{
	public class Events
	{
		[Fact]
		public async Task CanAppend()
		{
			using (var agg = new AggregationEngine())
			{
				for (int i = 0; i < 15; i++)
				{
					await agg.AppendAsync("test", new[] { "a" }, new RavenJObject { { "Item", i } });
				}
			}
		}

		[Fact]
		public async Task AfterRestartRememberLastEtag()
		{
			using (var pager = new PureMemoryPager())
			{
				long lastKey;
				using (var agg = new AggregationEngine(pager:pager))
				{
					for (int i = 0; i < 15; i++)
					{
						await agg.AppendAsync("test", new[] { "a" }, new RavenJObject {{"Item", i}});
					}

					Assert.NotEqual(0, agg.LastKey);

					lastKey = agg.LastKey;
					await agg.DisposeAsync();
				}

				using (var agg = new AggregationEngine(pager: pager))
				{
					Assert.Equal(lastKey, agg.LastKey);
					await agg.DisposeAsync();
				}
			}
		}

		[Fact]
		public async Task CanAppendParallel()
		{
			using (var agg = new AggregationEngine())
			{
				var tasks = new List<Task>();
				for (int i = 0; i < 15; i++)
				{
					tasks.Add(agg.AppendAsync("test", new[] { "a" }, new RavenJObject { { "Item", i } }));
				}
				await Task.WhenAll(tasks);
				await agg.DisposeAsync();
			}
		}

		[Fact]
		public async Task CanIterate()
		{
			using (var agg = new AggregationEngine())
			{
				for (int i = 0; i < 15; i++)
				{
					await agg.AppendAsync("test", new[] { "a" }, new RavenJObject { { "Val", i } });
				}

				int j = 0;
				foreach (var item in agg.Events(0))
				{
					Assert.Equal(j++, item.Data.Value<int>("Val"));
					Assert.NotEqual(0, item.Key);
				}
				await agg.DisposeAsync();
			}
		} 
	}
}