// -----------------------------------------------------------------------
//  <copyright file="WithTags.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Xunit;
using System.Linq;

namespace Raven.AggregationEngine.Tests
{
	public class WithTags
	{
		[Fact]
		public async Task CanAddThenReadByTag()
		{
			using (var agg = new AggregationEngine())
			{
				for (int i = 0; i < 15; i++)
				{
					for (int j = 0; j < 3; j++)
					{
						await agg.AppendAsync(new RavenJObject
						{
							{"Item", i},
							{"@metadata", new RavenJObject {{"Raven-Tags", new RavenJArray {"user/" + i}}}}
						});
					}
				}

				for (int i = 0; i < 15; i++)
				{
					Assert.Equal(3, agg.EventsByTag("user/" + i, 0).Count());
				}

				await agg.DisposeAsync();
			}
		} 
	}
}