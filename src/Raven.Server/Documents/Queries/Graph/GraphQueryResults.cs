using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Server.Documents.Queries.Graph
{
    public class GraphQueryResults
    {
        private List<GraphQueryRunner.Match> _results;
        private bool _alreadyInitialize;
        private bool _sealed;
        public void Initialize()
        {
            if (_alreadyInitialize)
            {
                _sealed = true;
                return;
            }
            _alreadyInitialize = true;
            _results = new List<GraphQueryRunner.Match>();
        }

        public void Add(GraphQueryRunner.Match match)
        {
            if(_sealed)
                return;
            _results.Add(match);
        }
        public GraphQueryRunner.Match this[int i] => _results[i];
        public int Count => _results?.Count??0;
        public List<GraphQueryRunner.Match> Results => _results;
    }
}
