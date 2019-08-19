using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Sorting.Js
{
    public class JsComparator : ScriptRunnerCache.Key, IComparer<BlittableJsonReaderObject>, IDisposable
    {
        private readonly string _script;
        private readonly DocumentDatabase _database;
        private readonly DocumentsOperationContext _ctx;
        private ScriptRunner.ReturnRun _scriptRunner;
        private ScriptRunner.SingleRun _run;
        public JsComparator(DocumentDatabase database, string compareScript, DocumentsOperationContext ctx /*might not be the right place*/)
        {
            _script = compareScript;
            _database = database;
            _ctx = ctx;
            _scriptRunner = _database.Scripts.GetScriptRunner(this, true, out _run);
        }
        public override void GenerateScript(ScriptRunner runner)
        {
            var mainScript = $@"
{_script}

function execute(_, x, y){{ 
     return compare.call(this, x, y);
}}";
            runner.AddScript(mainScript);
        }

        private const double NegativeEpsilon = -1 * double.Epsilon;
        public int Compare(BlittableJsonReaderObject x, BlittableJsonReaderObject y)
        {
            using (var scriptResult = _run.Run(_ctx, _ctx, "execute", new[] {null, x, y}))
            {
                var res = scriptResult.NumericValue;
                if(res.HasValue == false)
                    throw new InvalidDataException($"JsComparator: expected a return type of double but got something else.");
                if (res > double.Epsilon)
                    return 1;
                if (res < NegativeEpsilon)
                    return -1;
                return 0;

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

        public void Dispose()
        {

        }
    }
}
