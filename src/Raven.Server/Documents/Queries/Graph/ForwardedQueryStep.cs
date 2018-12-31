using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;

namespace Raven.Server.Documents.Queries.Graph
{
    public class ForwardedQueryStep : IGraphQueryStep
    {
        public ForwardedQueryStep(IGraphQueryStep forwardedStep, string alias)
        {
            _forwardedStep = forwardedStep;
            _aliasStr = alias;
            var originalAliases = _forwardedStep.GetAllAliases();
            if (originalAliases.Count != 1)
            {
                throw new NotSupportedException("Currently 'ForwardedQueryStep' doesn't support steps with multiple aliases associated with them");
            }

            _originalAlias = originalAliases.Single();
            CollectIntermediateResults = _forwardedStep.CollectIntermediateResults;
        }
        private IGraphQueryStep _forwardedStep;
        public IGraphQueryStep ForwardedStep => _forwardedStep;
        private bool _initialized;
        private string _aliasStr;
        private string _originalAlias;
        private List<GraphQueryRunner.Match> _results = new List<GraphQueryRunner.Match>();
        private Dictionary<string, GraphQueryRunner.Match> _resultsById = new Dictionary<string, GraphQueryRunner.Match>(StringComparer.OrdinalIgnoreCase);
        private HashSet<string> _allAliases;
        private int _index = -1;

        public ValueTask Initialize()
        {
            if (_forwardedStep.IsInitialized == false)
            {
                _forwardedStep.Initialize();                
            }
            //We expect the _forwardedStep to be initialized but not iterated here
            //TODO: this is just an ugly code to proof that this is needed this should be replaced with clone and rename _results

            var qqs = _forwardedStep as QueryQueryStep;
            qqs.Reset();

            while (qqs.GetNext(out var m))
            {
                var cloned = m.CloneAndReplaceAlias(_originalAlias, _aliasStr);
                _results.Add(cloned);
                foreach (var doc in cloned.GetAllResults())
                {
                    if (doc.Id == null)
                        continue;
                    _resultsById[doc.Id] = cloned;
                }
            }
            qqs.Reset();
            _index = 0;
            _initialized = true;
            return default;
        }

        public HashSet<string> GetAllAliases()
        {
            if (_allAliases != null)
                return _allAliases;
            //We don't want to modify the original step aliases 
            _allAliases = new HashSet<string> { _aliasStr };
            foreach (var alias in _forwardedStep.GetAllAliases())
            {
                if (alias == _originalAlias)
                    continue;
                _allAliases.Add(alias);
            }
            return _allAliases;
        }

        public string GetOutputAlias()
        {
            var originalOutputAlias = _forwardedStep.GetOutputAlias();
            if (originalOutputAlias  == _originalAlias)
                return _aliasStr;
            return originalOutputAlias;
        }

        public bool GetNext(out GraphQueryRunner.Match match)
        {
            if (_index >= _results.Count)
            {
                match = default;
                return false;
            }
            match = _results[_index++];

            return true;
        }

        private List<GraphQueryRunner.Match> _temp = new List<GraphQueryRunner.Match>();
        public List<GraphQueryRunner.Match> GetById(string id)
        {
            if (_results.Count != 0 && _resultsById.Count == 0)// only reason is that we are projecting non documents here
                throw new InvalidOperationException("Target vertices in a pattern match that originate from map/reduce WITH clause are not allowed. (pattern match has multiple statements in the form of (a)-[:edge]->(b) ==> in such pattern, 'b' must not originate from map/reduce index query)");

            _temp.Clear();
            if (_resultsById.TryGetValue(id, out var match))
                _temp.Add(match);
            return _temp;
        }

        public void Analyze(GraphQueryRunner.Match match, GraphQueryRunner.GraphDebugInfo graphDebugInfo)
        {
            //This will only work for QueryQuerySteps
            var result = match.GetResult(_aliasStr);
            if (result == null)
                return;

            if (result is Document d && d.Id != null)
            {
                graphDebugInfo.AddNode(d.Id.ToString(), d);
            }
            else
            {
                graphDebugInfo.AddNode(null, result);
            }
        }

        public bool IsEmpty()
        {
            return _results.Count == 0;
        }

        public bool CollectIntermediateResults { get; set; }


        public List<GraphQueryRunner.Match> IntermediateResults => _results;
        public IGraphQueryStep Clone()
        {
            var res = new ForwardedQueryStep(_forwardedStep.Clone(), _aliasStr);
            return res;
        }

        public ISingleGraphStep GetSingleGraphStepExecution()
        {
            return new ForwardedQuerySingleStep(this);
        }

        public bool IsInitialized => _initialized;
        public void Reset()
        {
            _index = 0;
        }

        private class ForwardedQuerySingleStep : ISingleGraphStep
        {
            private ForwardedQueryStep _parent;
            private List<GraphQueryRunner.Match> _temp = new List<GraphQueryRunner.Match>(1);

            public ForwardedQuerySingleStep(ForwardedQueryStep queryQueryStep)
            {
                _parent = queryQueryStep;
            }


            public void AddAliases(HashSet<string> aliases)
            {
                aliases.UnionWith(_parent.GetAllAliases());
            }

            public void SetPrev(IGraphQueryStep prev)
            {
            }

            public bool GetAndClearResults(List<GraphQueryRunner.Match> matches)
            {
                if (_temp.Count == 0)
                    return false;

                matches.AddRange(_temp);

                _temp.Clear();

                return true;
            }

            public ValueTask Initialize()
            {
                return _parent.Initialize();
            }

            public void Run(GraphQueryRunner.Match src, string alias)
            {
                // here we already get the right match, and we do nothing with it.
                var clone = new GraphQueryRunner.Match(src);
                clone.Remove(alias);
                clone.Set(_parent.GetOutputAlias(), src.GetResult(alias));
                _temp.Add(clone);
            }
        }
    }
}
