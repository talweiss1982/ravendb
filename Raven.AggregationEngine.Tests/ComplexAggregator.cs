using System;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Xunit;
using System.Linq;

namespace Raven.AggregationEngine.Tests
{
	public class ComplexAggregator
	{
		[Fact]
		public async Task UsingMultiMap()
		{
			using (var agg = new AggregationEngine())
			{
				
				agg.CreateAggregation(new IndexDefinition
					{
						Name = "TotalMonthlyBill",
						Maps =
							{
								"from sms in docs.Sms select new " +
								"{ Calls = 0, Sms = 1,  Key = sms.From + '/' + sms.At.Year  +'/' + sms.At.Month}",
								"from call in docs.Calls select new " +
								"{ Calls = call.Duration.TotalSeconds, Sms = 0, Key = call.From + '/' + call.At.Year  +'/' + call.At.Month }"
							},
						Reduce = "from result in results group result by result.Key into g " +
						         "select new {Calls = g.Sum(x=>x.Calls), Sms = g.Sum(x=>x.Sms), g.Key }"
					});


				for (int i = 0; i < 50; i++)
				{
					await agg.AppendAsync(new RavenJObject
						{
							{"@metadata", new RavenJObject{{"Raven-Entity-Name", "Sms"}}},
							{"From", "1234"},
							{"At", new DateTime(2013, 6, 19).AddDays(i).ToString("o")}
						});
				}

				for (int i = 0; i < 70; i++)
				{
					await agg.AppendAsync(
						new RavenJObject
						{
							{"@metadata", new RavenJObject{{"Raven-Entity-Name", "Calls"}}},
							{"From", "1234"},
							{"At", new DateTime(2013, 6, 19).AddDays(i).ToString("o")},
							{"Duration", TimeSpan.FromSeconds(i+4).ToString()}
						});
				}

				var aggregation = agg.GetAggregation("TotalMonthlyBill");
				await aggregation.WaitForEtagAsync(agg.LastKey);

				var datas = aggregation.AggregationResults().ToArray();

				Assert.Equal(3, datas.Length);

				await agg.DisposeAsync();
			}
		}

		
	}
}