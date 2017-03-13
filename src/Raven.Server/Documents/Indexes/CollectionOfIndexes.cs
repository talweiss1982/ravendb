﻿using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

using Sparrow.Collections;

namespace Raven.Server.Documents.Indexes
{
    public class CollectionOfIndexes : IEnumerable<Index>
    {
        private readonly ConcurrentDictionary<int, Index> _indexesById = new ConcurrentDictionary<int, Index>();
        private readonly ConcurrentDictionary<string, Index> _indexesByName = new ConcurrentDictionary<string, Index>(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, ConcurrentSet<Index>> _indexesByCollection = new ConcurrentDictionary<string, ConcurrentSet<Index>>(StringComparer.OrdinalIgnoreCase);
        private int _nextIndexId = 1;

        public void Add(Index index)
        {
            if (index.IndexId < _nextIndexId)
                throw new InvalidOperationException($"Invalid index Id. Should be equal or greater than {_nextIndexId}. Was {index.IndexId}");

            _nextIndexId = Math.Max(index.IndexId, _nextIndexId) + 1;
            _indexesById[index.IndexId] = index;
            _indexesByName[index.Name] = index;

            foreach (var collection in index.Definition.Collections)
            {
                var indexes = _indexesByCollection.GetOrAdd(collection, s => new ConcurrentSet<Index>());
                indexes.Add(index);
            }
        }

        public void ReplaceIndex(string name, Index oldIndex, Index newIndex)
        {
            Debug.Assert(oldIndex == null || string.Equals(name, oldIndex.Name, StringComparison.OrdinalIgnoreCase));

            _indexesByName.AddOrUpdate(name, oldIndex, (key, oldValue) => newIndex);

            Index _;
            _indexesByName.TryRemove(newIndex.Name, out _);

            if (oldIndex == null)
                return;

            foreach (var collection in oldIndex.Definition.Collections)
            {
                ConcurrentSet<Index> indexes;
                if (_indexesByCollection.TryGetValue(collection, out indexes) == false)
                    continue;

                indexes.TryRemove(oldIndex);
            }
            _indexesById.TryRemove(oldIndex.IndexId, out oldIndex);
        }

        public void RenameIndex(Index index, string oldName, string newName)
        {
            _indexesByName.AddOrUpdate(newName, index, (key, oldValue) => index);
            Index _;
            _indexesByName.TryRemove(oldName, out _);
        }

        public bool TryGetById(int id, out Index index)
        {
            return _indexesById.TryGetValue(id, out index);
        }

        public bool TryGetByName(string name, out Index index)
        {
            return _indexesByName.TryGetValue(name, out index);
        }

        public bool TryRemoveById(int id, out Index index)
        {
            var result = _indexesById.TryRemove(id, out index);
            if (result == false)
                return false;

            _indexesByName.TryRemove(index.Name, out index);

            foreach (var collection in index.Definition.Collections)
            {
                ConcurrentSet<Index> indexes;
                if (_indexesByCollection.TryGetValue(collection, out indexes) == false)
                    continue;

                indexes.TryRemove(index);
            }

            return true;
        }

        public IEnumerable<Index> GetForCollection(string collection)
        {
            ConcurrentSet<Index> indexes;

            if (_indexesByCollection.TryGetValue(collection, out indexes) == false)
                return Enumerable.Empty<Index>();

            return indexes;
        }

        public int GetNextIndexId()
        {
            return _nextIndexId;
        }

        public IEnumerator<Index> GetEnumerator()
        {
            return _indexesById.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int Count => _indexesById.Count;
    }
}