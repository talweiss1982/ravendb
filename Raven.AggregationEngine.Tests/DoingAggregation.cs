using System;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Voron.Impl;
using Xunit;
using System.Linq;

namespace Raven.AggregationEngine.Tests
{
	public class DoingAggregation : IDisposable
	{
		[Fact]
		public async Task CanAdd()
		{
			using (var agg = new AggregationEngine())
			{
				agg.CreateAggregation(new IndexDefinition
				{
					Name = "Test",
					Map = "from doc in docs select new { Count = 1}",
					Reduce = "from result in results group result by 1 into g select new { Count = g.Sum(x=>x.Count) }"
				});

				long last = 0;
				for (int j = 0; j < 3; j++)
				{
					for (int i = 0; i < 15; i++)
					{
						last = await agg.AppendAsync(new RavenJObject { { "Item", i } });
					}

					var aggregation = agg.GetAggregation("test");

					await aggregation.WaitForEtagAsync(last);


					var eventDatas = agg.Events(0).ToList();
					Assert.Equal(15 * (j + 1), eventDatas.Count);
					
					var result = aggregation.AggregationResultFor("1");
					Assert.Equal(eventDatas.Count, result.Value<int>("Count"));
				}
				await agg.DisposeAsync();
			}
		}

		[Fact]
		public async Task WillRememberAfterRestart()
		{
			long last = 0;
			using (var pager = new PureMemoryPager())
			{
				using (var agg = new AggregationEngine(pager: pager))
				{
					agg.CreateAggregation(new IndexDefinition
					{
						Name = "Test",
						Map = "from doc in docs select new { Count = 1}",
						Reduce = "from result in results group result by 1 into g select new { Count = g.Sum(x=>x.Count) }"
					});

					var aggregation = agg.GetAggregation("test");
					for (int j = 0; j < 3; j++)
					{
						long lastWrite = 0;
						for (int i = 0; i < 15; i++)
						{
							lastWrite = await agg.AppendAsync(new RavenJObject { { "Item", i } });
						}


						await aggregation.WaitForEtagAsync(lastWrite);

						var result = aggregation.AggregationResultFor("1");

						Assert.Equal(15*(j + 1), result.Value<int>("Count"));
					}

					last = aggregation.LastAggregatedEtag;
					await agg.DisposeAsync();
				}

				using (var agg = new AggregationEngine(pager: pager))
				{
					var aggregation = agg.GetAggregation("test");
					var result = aggregation.AggregationResultFor("1");

					Assert.Equal(last, aggregation.LastAggregatedEtag);
					Assert.Equal(45, result.Value<int>("Count"));

					await agg.DisposeAsync();
				}
			}
		}

		public void Dispose()
		{
		}
	}
}