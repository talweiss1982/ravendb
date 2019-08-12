using System;
using System.Collections.Generic;
using System.Text;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Sorting.Js
{
    public class JsComparator : ScriptRunnerCache.Key, IComparer<BlittableJsonReaderObject>
    {
        private readonly string _script;
        private readonly DocumentDatabase _database;
        private readonly DocumentsOperationContext _ctx;

        public JsComparator(DocumentDatabase database, string compareScript, DocumentsOperationContext ctx /*might not be the right place*/)
        {
            _script = compareScript;
            _database = database;
            _ctx = ctx;
        }
        public override void GenerateScript(ScriptRunner runner)
        {
            var mainScript = $@"
{_script}

function execute(_ , args){{ 
     return compare.call(null , args);
}}";
            runner.AddScript(mainScript);
        }

        public int Compare(BlittableJsonReaderObject x, BlittableJsonReaderObject y)
        {
            using (_database.Scripts.GetScriptRunner(this, true, out var singleRun))
            {
                using (var scriptResult = singleRun.Run(_ctx, _ctx, "execute", new[] {x, y}))
                {
                    var res = scriptResult.TranslateToObject(_ctx);
                    return 1;
                }
            }
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;

            if (ReferenceEquals(this, obj))
                return true;

            if (obj is JsComparator other)
            {
                return string.Equals(_script, other._script);
            }

            return false;
        }

        public override int GetHashCode()
        {
            return _script.GetHashCode();
        }
    }
}
