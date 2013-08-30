using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Voron.Impl;
using Xunit;

namespace Raven.AggregationEngine.Tests
{
	public class AggregationsCreation
	{
		[Fact]
		public void CanAdd()
		{
			using (var agg = new AggregationEngine())
			{
				agg.CreateAggregation(new IndexDefinition
					{
						Name = "Test",
						Map = "from doc in docs select new { Count = 1}",
						Reduce = "from result in results group result by 1 into g select new { Count = g.Sum(x=>x.Count) }"
					});
				Assert.NotNull(agg.GetAggregation("Test"));
			}
		}

		[Fact]
		public async Task CanAddAndRememberAfterRestart()
		{
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
					Assert.NotNull(agg.GetAggregation("Test"));

					await agg.DisposeAsync();
				}

				using (var agg = new AggregationEngine(pager: pager))
				{
					Assert.NotNull(agg.GetAggregation("Test"));
				}
			}
		}

		[Fact]
		public void CanUpdate()
		{
			using (var agg = new AggregationEngine())
			{
				agg.CreateAggregation(new IndexDefinition
			   {
				   Name = "Test",
				   Map = "from doc in docs select new { Count = 1}",
				   Reduce = "from result in results group result by 1 into g select new { Count = g.Sum(x=>x.Count) }"
			   });

				var indexDefinition = new IndexDefinition
					{
						Name = "Test",
						Map = "from doc in docs select new { Sum = 1}",
						Reduce = "from result in results group result by 1 into g select new { Sum = g.Sum(x=>x.Sum) }"
					};
				agg.CreateAggregation(indexDefinition);

				Assert.Contains(indexDefinition.Reduce, agg.GetAggregation("Test").Generator.ViewText);
			}
		}
	}
}
