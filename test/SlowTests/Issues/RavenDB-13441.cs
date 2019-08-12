using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields;
using Raven.Server.Documents.Queries.Sorting.Js;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13441 : RavenLowLevelTestBase
    {
        [Fact]
        public void CanUseJsComparer()
        {
            List<BlittableJsonReaderObject> points = null;
            try
            {
                using (var db = CreateDocumentDatabase())
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                {
                    points = new List<BlittableJsonReaderObject>();
                    for (int i = 0; i < 10; i++)
                    {
                        points.Add(ctx.ReadObject(Point.GenerateNew(), "RavenDB_13441"));
                    }

                    var script = @"
                        function distance(x){
                            return Math.sqrt(Math.pow(x.X, 2) + Math.pow(x.Y, 2))
                        }
                        function compare(x, y){
                            return distance(x) - distance(y)
                        }
";
                    points.Sort(new JsComparator(db, script, ctx));
                }
            }
            finally
            {
                foreach (var b in points?? Enumerable.Empty<BlittableJsonReaderObject>())
                {
                    b.Dispose();
                }
            }
        }

        private class Point
        {
            public int X { get; set; }
            public int Y { get; set; }
            private static Random rand = new Random(1223);
            public static DynamicJsonValue GenerateNew()
            {
                return new DynamicJsonValue
                {
                    ["X"] = rand.Next(0,100),
                    ["Y"] = rand.Next(0,100)
                };
            }
        }
    }
}
