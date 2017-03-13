﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow;
using Voron.Data.Compression;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.BTrees
{
    public unsafe partial class Tree : IDisposable
    {
        private int _directAddUsage;

#if VALIDATE_DIRECT_ADD_STACKTRACE
        private string _allocationStacktrace;
#endif

        private readonly TreeMutableState _state;
        private readonly bool _isPageLocatorOwned;
        private readonly RecentlyFoundTreePages _recentlyFoundPages;

        private Dictionary<Slice, FixedSizeTree> _fixedSizeTrees;
        private PageLocator _pageLocator;

        public event Action<long, PageFlags> PageModified;
        public event Action<long, PageFlags> PageFreed;

        public Slice Name { get; private set; }

        public TreeMutableState State => _state;

        private readonly LowLevelTransaction _llt;
        private readonly Transaction _tx;
        private readonly NewPageAllocator _newPageAllocator;

        public LowLevelTransaction Llt => _llt;

        private Tree(LowLevelTransaction llt, Transaction tx, long root, Slice name, NewPageAllocator newPageAllocator = null, PageLocator pageLocator = null)
        {
            _llt = llt;
            _tx = tx;
            Name = name;
            _newPageAllocator = newPageAllocator;
            _recentlyFoundPages = new RecentlyFoundTreePages(llt.Flags == TransactionFlags.Read ? 8 : 2);
            _isPageLocatorOwned = pageLocator == null;
            _pageLocator = pageLocator ?? llt.PersistentContext.AllocatePageLocator(llt);

            _state = new TreeMutableState(llt)
            {
                RootPageNumber = root
            };
        }

        public Tree(LowLevelTransaction llt, Transaction tx, Slice name, TreeMutableState state)
        {
            _llt = llt;
            _tx = tx;
            Name = name;
            _recentlyFoundPages = new RecentlyFoundTreePages(llt.Flags == TransactionFlags.Read ? 8 : 2);
            _isPageLocatorOwned = true;
            _pageLocator = llt.PersistentContext.AllocatePageLocator(llt);
            _state = new TreeMutableState(llt);
            _state = state;
        }

        public bool IsLeafCompressionSupported
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (State.Flags & TreeFlags.LeafsCompressed) == TreeFlags.LeafsCompressed; }
        }

        public static Tree Open(LowLevelTransaction llt, Transaction tx, Slice name, TreeRootHeader* header, RootObjectType type = RootObjectType.VariableSizeTree,
             NewPageAllocator newPageAllocator = null, PageLocator pageLocator = null)
        {
            return new Tree(llt, tx, header->RootPageNumber, name, newPageAllocator, pageLocator)
            {
                _state =
                {
                    RootObjectType = type,
                    PageCount = header->PageCount,
                    BranchPages = header->BranchPages,
                    Depth = header->Depth,
                    OverflowPages = header->OverflowPages,
                    LeafPages = header->LeafPages,
                    NumberOfEntries = header->NumberOfEntries,
                    Flags = header->Flags
                }
            };
        }

        public static Tree Create(LowLevelTransaction llt, Transaction tx, Slice name, TreeFlags flags = TreeFlags.None, RootObjectType type = RootObjectType.VariableSizeTree,
             NewPageAllocator newPageAllocator = null,
             PageLocator pageLocator = null)
        {
            if (type != RootObjectType.VariableSizeTree && type != RootObjectType.Table)
                ThrowInvalidTreeCreateType();

            var newPage = newPageAllocator?.AllocateSinglePage(0) ?? llt.AllocatePage(1);

            TreePage newRootPage = PrepareTreePage(TreePageFlags.Leaf, 1, newPage);

            var tree = new Tree(llt, tx, newRootPage.PageNumber, name, newPageAllocator, pageLocator)
            {
                _state =
                {
                    RootObjectType = type,
                    Depth = 1,
                    Flags = flags,
                }
            };

            if ((flags & TreeFlags.LeafsCompressed) == TreeFlags.LeafsCompressed)
                tree.InitializeCompression();

            tree.State.RecordNewPage(newRootPage, 1);
            return tree;
        }

        private static void ThrowInvalidTreeCreateType()
        {
            throw new ArgumentException(
                $"Only valid types are {nameof(RootObjectType.VariableSizeTree)} or {nameof(RootObjectType.Table)}.",
                "type");
        }

        /// <summary>
        /// This is using little endian
        /// </summary>
        public long Increment(Slice key, long delta)
        {
            State.IsModified = true;

            long currentValue = 0;

            var read = Read(key);
            if (read != null)
                currentValue = *(long*)read.Reader.Base;

            var value = currentValue + delta;
            byte* ptr;
            using (DirectAdd(key, sizeof(long), out ptr))
                *(long*)ptr = value;

            return value;
        }

        /// <summary>
        /// This is using little endian
        /// </summary>
        public bool AddMax(Slice key, long value)
        {
            var read = Read(key);
            if (read != null)
            {
                var currentValue = *(long*)read.Reader.Base;
                if (currentValue >= value)
                    return false;
            }

            State.IsModified = true;

            byte* ptr;
            using (DirectAdd(key, sizeof(long), out ptr))
                *(long*)ptr = value;

            return true;
        }

        public void Add(Slice key, Stream value)
        {
            ValidateValueLength(value);

            State.IsModified = true;

            var length = (int)value.Length;

            byte* ptr;
            using (DirectAdd(key, length, out ptr))
                CopyStreamToPointer(_llt, value, ptr);
        }

        private static void ValidateValueLength(Stream value)
        {
            if (value == null)
                ThrowNullReferenceException();
            Debug.Assert(value != null);
            if (value.Length > int.MaxValue)
                ThrowValueTooLarge();
        }

        private static void ThrowValueTooLarge()
        {
            throw new ArgumentException("Cannot add a value that is over 2GB in size");
        }

        private static void ThrowNullReferenceException()
        {
            throw new ArgumentNullException();
        }

        public void Add(Slice key, byte[] value)
        {
            if (value == null)
                ThrowNullReferenceException();
            Debug.Assert(value != null);

            State.IsModified = true;
            byte* ptr;
            using (DirectAdd(key, value.Length, out ptr))
            {
                fixed (byte* src = value)
                {
                    Memory.Copy(ptr, src, value.Length);
                }
            }
        }

        public void Add(Slice key, Slice value)
        {
            if (!value.HasValue)
                ThrowNullReferenceException();

            State.IsModified = true;
            byte* ptr;
            using (DirectAdd(key, value.Size, out ptr))
                value.CopyTo(ptr);
        }

        private static void CopyStreamToPointer(LowLevelTransaction tx, Stream value, byte* pos)
        {
            TemporaryPage tmp;
            using (tx.Environment.GetTemporaryPage(tx, out tmp))
            {
                var tempPageBuffer = tmp.TempPageBuffer;
                var tempPagePointer = tmp.TempPagePointer;

                while (true)
                {
                    var read = value.Read(tempPageBuffer, 0, tempPageBuffer.Length);
                    if (read == 0)
                        break;

                    Memory.CopyInline(pos, tempPagePointer, read);
                    pos += read;

                    if (read != tempPageBuffer.Length)
                        break;
                }
            }
        }

        public static int CalcSizeOfEmbeddedEntry(int keySize, int entrySize)
        {
            var size = (Constants.Tree.NodeHeaderSize + keySize + entrySize);
            return size + (size & 1);
        }

        public DirectAddScope DirectAdd(Slice key, int len, out byte* ptr)
        {
            return DirectAdd(key, len, TreeNodeFlags.Data, out ptr);
        }

        public DirectAddScope DirectAdd(Slice key, int len, TreeNodeFlags nodeType, out byte* ptr)
        {
            Debug.Assert(nodeType == TreeNodeFlags.Data || nodeType == TreeNodeFlags.MultiValuePageRef);

            if (_llt.Flags == TransactionFlags.ReadWrite)
            {
                State.IsModified = true;
            }
            else
            {
                ThreadCannotAddInReadTx();
            }

            if (AbstractPager.IsKeySizeValid(key.Size) == false)
                ThrowInvalidKeySize(key);
            
            TreeNodeHeader* node;
            TreeCursorConstructor cursorConstructor;
            var foundPage = FindPageFor(key, node: out node, cursor: out cursorConstructor, allowCompressed: true);

            var page = ModifyPage(foundPage);

            bool? shouldGoToOverflowPage = null;
            if (page.LastMatch == 0) // this is an update operation
            {
                node = page.GetNode(page.LastSearchPosition);

#if DEBUG
                Slice nodeCheck;
                using (TreeNodeHeader.ToSlicePtr(_llt.Allocator, node, out nodeCheck))
                {
                    Debug.Assert(SliceComparer.EqualsInline(nodeCheck, key));
                }
#endif
                shouldGoToOverflowPage = ShouldGoToOverflowPage(len);

                byte* pos;
                if (shouldGoToOverflowPage == false)
                {
                    // optimization for Data and MultiValuePageRef - try to overwrite existing node space
                    if (TryOverwriteDataOrMultiValuePageRefNode(node, len, nodeType, out pos))
                    {
                        ptr = pos;
                        return new DirectAddScope(this);
                    }
                }
                else
                {
                    // optimization for PageRef - try to overwrite existing overflows
                    if (TryOverwriteOverflowPages(node, len, out pos))
                    {
                        ptr = pos;
                        return new DirectAddScope(this);
                    }
                }

                RemoveLeafNode(page);
            }
            else // new item should be recorded
            {
                State.NumberOfEntries++;
            }

            var lastSearchPosition = page.LastSearchPosition; // searching for overflow pages might change this
            byte* overFlowPos = null;
            var pageNumber = -1L;
            if (shouldGoToOverflowPage ?? ShouldGoToOverflowPage(len))
            {
                pageNumber = WriteToOverflowPages(len, out overFlowPos);
                len = -1;
                nodeType = TreeNodeFlags.PageRef;
            }

            byte* dataPos;
            if (page.HasSpaceFor(_llt, key, len) == false)
            {
                if (IsLeafCompressionSupported == false || TryCompressPageNodes(key, len, page) == false)
                {
                    using (var cursor = cursorConstructor.Build(key))
                    {
                        cursor.Update(cursor.Pages.First, page);

                        var pageSplitter = new TreePageSplitter(_llt, this, key, len, pageNumber, nodeType, cursor);
                        dataPos = pageSplitter.Execute();
                    }

                    DebugValidateTree(State.RootPageNumber);

                    ptr = overFlowPos == null ? dataPos : overFlowPos;
                    return new DirectAddScope(this);
                }

                // existing values compressed and put at the end of the page, let's insert from Upper position
                lastSearchPosition = 0;
            }

            switch (nodeType)
            {
                case TreeNodeFlags.PageRef:
                    dataPos = page.AddPageRefNode(lastSearchPosition, key, pageNumber);
                    break;
                case TreeNodeFlags.Data:
                    dataPos = page.AddDataNode(lastSearchPosition, key, len);
                    break;
                case TreeNodeFlags.MultiValuePageRef:
                    dataPos = page.AddMultiValueNode(lastSearchPosition, key, len);
                    break;
                default:
                    ThrowUnknownNodeTypeAddOperation(nodeType);
                    dataPos = null; // never executed
                    break;
            }

            page.DebugValidate(this, State.RootPageNumber);

            ptr = overFlowPos == null ? dataPos : overFlowPos;
            return new DirectAddScope(this);
        }

        public struct DirectAddScope : IDisposable
        {
            private readonly Tree _parent;

            public DirectAddScope(Tree parent)
            {
                _parent = parent;
                if (_parent._directAddUsage++ != 0)
                {
                    ThrowScopeAlreadyOpen();
                }

#if VALIDATE_DIRECT_ADD_STACKTRACE
                _parent._allocationStacktrace = Environment.StackTrace;
#endif
            }

            public void Dispose()
            {
                _parent._directAddUsage--;
            }


            private void ThrowScopeAlreadyOpen()
            {
                var message = $"Write operation already requested on a tree name: {_parent}. " +
                              $"{nameof(Tree.DirectAdd)} method cannot be called recursively while the scope is already opened.";

#if VALIDATE_DIRECT_ADD_STACKTRACE
                message += Environment.NewLine + _parent._allocationStacktrace;
#endif

                throw new InvalidOperationException(message);
            }

        }

        private static void ThrowUnknownNodeTypeAddOperation(TreeNodeFlags nodeType)
        {
            throw new NotSupportedException("Unknown node type for direct add operation: " + nodeType);
        }

        private static void ThrowInvalidKeySize(Slice key)
        {
            throw new ArgumentException(
                $"Key size is too big, must be at most {AbstractPager.MaxKeySize} bytes, but was {(key.Size + AbstractPager.RequiredSpaceForNewNode)}",
                nameof(key));
        }

        private static void ThreadCannotAddInReadTx()
        {
            throw new ArgumentException("Cannot add a value in a read only transaction");
        }

        public TreePage ModifyPage(TreePage page)
        {
            if (page.Dirty)
                return page;

            var newPage = ModifyPage(page.PageNumber);
            newPage.LastSearchPosition = page.LastSearchPosition;
            newPage.LastMatch = page.LastMatch;

            return newPage;
        }

        public TreePage ModifyPage(long pageNumber)
        {
            var newPage = GetWriteableTreePage(pageNumber);
            newPage.Dirty = true;
            _recentlyFoundPages.Reset(pageNumber);

            if (IsLeafCompressionSupported && newPage.IsCompressed)
                DecompressionsCache.Invalidate(pageNumber, DecompressionUsage.Read);

            PageModified?.Invoke(pageNumber, newPage.Flags);

            return newPage;
        }

        public bool ShouldGoToOverflowPage(int len)
        {
            return len + Constants.Tree.NodeHeaderSize > _llt.DataPager.NodeMaxSize;
        }

        private long WriteToOverflowPages(int overflowSize, out byte* dataPos)
        {
            var numberOfPages = _llt.DataPager.GetNumberOfOverflowPages(overflowSize);
            var newPage = _llt.AllocatePage(numberOfPages);

            TreePage overflowPageStart = PrepareTreePage(TreePageFlags.Value, numberOfPages, newPage);

            overflowPageStart.Flags = PageFlags.Overflow | PageFlags.VariableSizeTreePage;
            overflowPageStart.OverflowSize = overflowSize;
            dataPos = overflowPageStart.Base + Constants.Tree.PageHeaderSize;

            State.RecordNewPage(overflowPageStart, numberOfPages);

            PageModified?.Invoke(overflowPageStart.PageNumber, overflowPageStart.Flags);

            return overflowPageStart.PageNumber;
        }

        internal void RemoveLeafNode(TreePage page)
        {
            var node = page.GetNode(page.LastSearchPosition);
            if (node->Flags == (TreeNodeFlags.PageRef)) // this is an overflow pointer
            {
                var overflowPage = GetReadOnlyTreePage(node->PageNumber);
                FreePage(overflowPage);
            }

            page.RemoveNode(page.LastSearchPosition);
        }

        [Conditional("VALIDATE")]
        public void DebugValidateTree(long rootPageNumber)
        {
            var pages = new HashSet<long>();
            var stack = new Stack<TreePage>();
            var root = GetReadOnlyTreePage(rootPageNumber);
            stack.Push(root);
            pages.Add(rootPageNumber);
            while (stack.Count > 0)
            {
                var p = stack.Pop();

                using (p.IsCompressed ? (DecompressedLeafPage)(p = DecompressPage(p, skipCache: true)) : null)
                {
                    if (p.NumberOfEntries == 0 && p != root)
                    {
                        DebugStuff.RenderAndShowTree(this, rootPageNumber);
                        throw new InvalidOperationException("The page " + p.PageNumber + " is empty");

                    }
                    p.DebugValidate(this, rootPageNumber);
                    if (p.IsBranch == false)
                        continue;

                    if (p.NumberOfEntries < 2)
                    {
                        throw new InvalidOperationException("The branch page " + p.PageNumber + " has " +
                                                            p.NumberOfEntries + " entry");
                    }

                    for (int i = 0; i < p.NumberOfEntries; i++)
                    {
                        var page = p.GetNode(i)->PageNumber;
                        if (pages.Add(page) == false)
                        {
                            DebugStuff.RenderAndShowTree(this, rootPageNumber);
                            throw new InvalidOperationException("The page " + page + " already appeared in the tree!");
                        }
                        stack.Push(GetReadOnlyTreePage(page));
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal TreePage GetReadOnlyTreePage(long pageNumber)
        {
            var page = _pageLocator.GetReadOnlyPage(pageNumber);
            return new TreePage(page.Pointer, Constants.Storage.PageSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Page GetReadOnlyPage(long pageNumber)
        {
            return _pageLocator.GetReadOnlyPage(pageNumber);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal TreePage GetWriteableTreePage(long pageNumber)
        {
            var page = _pageLocator.GetWritablePage(pageNumber);
            return new TreePage(page.Pointer, Constants.Storage.PageSize);
        }

        internal TreePage FindPageFor(Slice key, out TreeNodeHeader* node)
        {
            TreePage p;

            if (TryUseRecentTransactionPage(key, out p, out node))
            {
                return p;
            }

            return SearchForPage(key, out node);
        }

        internal TreePage FindPageFor(Slice key, out TreeNodeHeader* node, out TreeCursorConstructor cursor, bool allowCompressed = false)
        {
            TreePage p;

            if (TryUseRecentTransactionPage(key, out cursor, out p, out node))
            {
                if (allowCompressed == false && p.IsCompressed)
                    ThrowOnCompressedPage(p);

                return p;
            }

            return SearchForPage(key, allowCompressed, out cursor, out node);
        }

        private TreePage SearchForPage(Slice key, out TreeNodeHeader* node)
        {
            var p = GetReadOnlyTreePage(State.RootPageNumber);

            var cursorPath = new List<long>();
            cursorPath.Add(p.PageNumber);

            bool rightmostPage = true;
            bool leftmostPage = true;

            while ((p.TreeFlags & TreePageFlags.Branch) == TreePageFlags.Branch)
            {
                int nodePos;

                if (key.Options == SliceOptions.Key)
                {
                    if (p.Search(_llt, key) != null)
                    {
                        nodePos = p.LastSearchPosition;
                        if (p.LastMatch != 0)
                        {
                            nodePos--;
                            p.LastSearchPosition--;
                        }

                        if (nodePos != 0)
                            leftmostPage = false;

                        rightmostPage = false;
                    }
                    else
                    {
                        nodePos = (ushort)(p.LastSearchPosition - 1);

                        leftmostPage = false;
                    }
                }
                else if (key.Options == SliceOptions.BeforeAllKeys)
                {
                    p.LastSearchPosition = nodePos = 0;
                    rightmostPage = false;
                }
                else // if (key.Options == SliceOptions.AfterAllKeys)
                {
                    p.LastSearchPosition = nodePos = (ushort)(p.NumberOfEntries - 1);
                    leftmostPage = false;
                }

                var pageNode = p.GetNode(nodePos);
                p = GetReadOnlyTreePage(pageNode->PageNumber);
                Debug.Assert(pageNode->PageNumber == p.PageNumber,
                    string.Format("Requested Page: #{0}. Got Page: #{1}", pageNode->PageNumber, p.PageNumber));

                cursorPath.Add(p.PageNumber);
            }

            if (p.IsLeaf == false)
                VoronUnrecoverableErrorException.Raise(_llt.Environment, "Index points to a non leaf page " + p.PageNumber);

            if (p.IsCompressed)
                ThrowOnCompressedPage(p);

            node = p.Search(_llt, key); // will set the LastSearchPosition

            AddToRecentlyFoundPages(cursorPath, p, leftmostPage, rightmostPage);

            return p;
        }

        private TreePage SearchForPage(Slice key, bool allowCompressed, out TreeCursorConstructor cursorConstructor, out TreeNodeHeader* node, bool addToRecentlyFoundPages = true)
        {
            var p = GetReadOnlyTreePage(State.RootPageNumber);

            var cursor = new TreeCursor();
            cursor.Push(p);

            bool rightmostPage = true;
            bool leftmostPage = true;

            while ((p.TreeFlags & TreePageFlags.Branch) == TreePageFlags.Branch)
            {
                int nodePos;
                if (key.Options == SliceOptions.BeforeAllKeys)
                {
                    p.LastSearchPosition = nodePos = 0;
                    rightmostPage = false;
                }
                else if (key.Options == SliceOptions.AfterAllKeys)
                {
                    p.LastSearchPosition = nodePos = (ushort)(p.NumberOfEntries - 1);
                    leftmostPage = false;
                }
                else
                {
                    if (p.Search(_llt, key) != null)
                    {
                        nodePos = p.LastSearchPosition;
                        if (p.LastMatch != 0)
                        {
                            nodePos--;
                            p.LastSearchPosition--;
                        }

                        if (nodePos != 0)
                            leftmostPage = false;

                        rightmostPage = false;
                    }
                    else
                    {
                        nodePos = (ushort)(p.LastSearchPosition - 1);

                        leftmostPage = false;
                    }
                }

                var pageNode = p.GetNode(nodePos);
                p = GetReadOnlyTreePage(pageNode->PageNumber);
                Debug.Assert(pageNode->PageNumber == p.PageNumber,
                    string.Format("Requested Page: #{0}. Got Page: #{1}", pageNode->PageNumber, p.PageNumber));

                cursor.Push(p);
            }

            cursorConstructor = new TreeCursorConstructor(cursor);

            if (p.IsLeaf == false)
                VoronUnrecoverableErrorException.Raise(_llt.Environment, "Index points to a non leaf page");

            if (allowCompressed == false && p.IsCompressed)
                ThrowOnCompressedPage(p);

            node = p.Search(_llt, key); // will set the LastSearchPosition

            if (p.NumberOfEntries > 0 && addToRecentlyFoundPages) // compressed page can have no ordinary entries
                AddToRecentlyFoundPages(cursor, p, leftmostPage, rightmostPage);

            return p;
        }

        private static void ThrowOnCompressedPage(TreePage p)
        {
            throw new InvalidOperationException($"Page {p.PageNumber} is compressed. You need to decompress it to be able to access its content.");
        }

        private void AddToRecentlyFoundPages(List<long> c, TreePage p, bool leftmostPage, bool rightmostPage)
        {
            Debug.Assert(p.IsCompressed == false);

            ByteStringContext.Scope firstScope, lastScope;
            Slice firstKey;
            if (leftmostPage)
            {
                firstScope = new ByteStringContext<ByteStringMemoryCache>.Scope();
                firstKey = Slices.BeforeAllKeys;
            }
            else
            {
                // We are going to store the slice, therefore we copy.
                firstScope = p.GetNodeKey(_llt, 0, ByteStringType.Immutable, out firstKey);
            }

            Slice lastKey;
            if (rightmostPage)
            {
                lastScope = new ByteStringContext<ByteStringMemoryCache>.Scope();
                lastKey = Slices.AfterAllKeys;
            }
            else
            {
                // We are going to store the slice, therefore we copy.
                lastScope = p.GetNodeKey(_llt, p.NumberOfEntries - 1, ByteStringType.Immutable, out lastKey);
            }

            var foundPage = new RecentlyFoundTreePages.FoundTreePage(p.PageNumber, p, firstKey, lastKey, c.ToArray(), firstScope, lastScope);

            _recentlyFoundPages.Add(foundPage);
        }

        private void AddToRecentlyFoundPages(TreeCursor c, TreePage p, bool leftmostPage, bool rightmostPage)
        {
            ByteStringContext.Scope firstScope, lastScope;
            Slice firstKey;
            if (leftmostPage)
            {
                firstScope = new ByteStringContext<ByteStringMemoryCache>.Scope();
                firstKey = Slices.BeforeAllKeys;
            }
            else
            {
                // We are going to store the slice, therefore we copy.
                firstScope = p.GetNodeKey(_llt, 0, ByteStringType.Immutable, out firstKey);
            }

            Slice lastKey;
            if (rightmostPage)
            {
                lastScope = new ByteStringContext<ByteStringMemoryCache>.Scope();
                lastKey = Slices.AfterAllKeys;
            }
            else
            {
                // We are going to store the slice, therefore we copy.
                lastScope = p.GetNodeKey(_llt, p.NumberOfEntries - 1, ByteStringType.Immutable, out lastKey);
            }

            var cursorPath = new long[c.Pages.Count];

            var cur = c.Pages.First;
            int pos = cursorPath.Length - 1;
            while (cur != null)
            {
                cursorPath[pos--] = cur.Value.PageNumber;
                cur = cur.Next;
            }

            var foundPage = new RecentlyFoundTreePages.FoundTreePage(p.PageNumber, p, firstKey, lastKey, cursorPath, firstScope, lastScope);

            _recentlyFoundPages.Add(foundPage);
        }

        private bool TryUseRecentTransactionPage(Slice key, out TreePage page, out TreeNodeHeader* node)
        {
            node = null;
            page = null;

            var foundPage = _recentlyFoundPages?.Find(key);
            if (foundPage == null)
                return false;

            if (foundPage.Page != null)
            {
                // we can't share the same instance, Page instance may be modified by
                // concurrently run iterators
                page = new TreePage(foundPage.Page.Base, foundPage.Page.PageSize);
            }
            else
            {
                page = GetReadOnlyTreePage(foundPage.Number);
            }

            if (page.IsLeaf == false)
                VoronUnrecoverableErrorException.Raise(_llt.Environment, "Index points to a non leaf page");

            node = page.Search(_llt, key); // will set the LastSearchPosition

            return true;
        }

        private bool TryUseRecentTransactionPage(Slice key, out TreeCursorConstructor cursor, out TreePage page, out TreeNodeHeader* node)
        {
            var foundPage = _recentlyFoundPages?.Find(key);
            if (foundPage == null)
            {
                page = null;
                node = null;
                cursor = default(TreeCursorConstructor);
                return false;
            }

            var lastFoundPageNumber = foundPage.Number;

            if (foundPage.Page != null)
            {
                // we can't share the same instance, Page instance may be modified by
                // concurrently run iterators
                page = new TreePage(foundPage.Page.Base, foundPage.Page.PageSize);
            }
            else
            {
                page = GetReadOnlyTreePage(lastFoundPageNumber);
            }

            if (page.IsLeaf == false)
                VoronUnrecoverableErrorException.Raise(_llt.Environment, "Index points to a non leaf page");

            node = page.Search(_llt, key); // will set the LastSearchPosition

            cursor = new TreeCursorConstructor(_llt, this, page, foundPage.CursorPath, lastFoundPageNumber);
            return true;
        }

        internal TreePage NewPage(TreePageFlags flags, long nearbyPage)
        {
            var newPage = _newPageAllocator?.AllocateSinglePage(nearbyPage) ?? _llt.AllocatePage(1);

            var page = PrepareTreePage(flags, 1, newPage);

            State.RecordNewPage(page, 1);

            PageModified?.Invoke(page.PageNumber, page.Flags);

            return page;
        }

        private static TreePage PrepareTreePage(TreePageFlags flags, int num, Page newPage)
        {
            var page = new TreePage(newPage.Pointer, Constants.Storage.PageSize)
            {
                Flags = PageFlags.VariableSizeTreePage | (num == 1 ? PageFlags.Single : PageFlags.Overflow),
                Lower = Constants.Tree.PageHeaderSize,
                TreeFlags = flags,
                Upper = Constants.Storage.PageSize,
                Dirty = true
            };
            return page;
        }


        internal void FreePage(TreePage p)
        {
#if VALIDATE
            p.Freed = true;
#endif
            PageFreed?.Invoke(p.PageNumber, p.Flags);

            if (p.IsOverflow)
            {
                var numberOfPages = _llt.DataPager.GetNumberOfOverflowPages(p.OverflowSize);
                for (int i = 0; i < numberOfPages; i++)
                {
                    _llt.FreePage(p.PageNumber + i);
                    _pageLocator.Reset(p.PageNumber + i);
                }

                State.RecordFreedPage(p, numberOfPages);
            }
            else
            {
                if (_newPageAllocator != null)
                    _newPageAllocator.FreePage(p.PageNumber);
                else
                    _llt.FreePage(p.PageNumber);
                _pageLocator.Reset(p.PageNumber);
                State.RecordFreedPage(p, 1);
            }
        }

        public void Delete(Slice key)
        {
            if (_llt.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot delete a value in a read only transaction");

            State.IsModified = true;            
            TreeNodeHeader* node;
            TreeCursorConstructor cursorConstructor;
            var page = FindPageFor(key, node: out node, cursor: out cursorConstructor, allowCompressed: true);

            if (page.IsCompressed)
            {
                DeleteOnCompressedPage(page, key, ref cursorConstructor);
                return;
            }

            if (page.LastMatch != 0)
                return; // not an exact match, can't delete

            page = ModifyPage(page);

            State.NumberOfEntries--;

            RemoveLeafNode(page);

            using (var cursor = cursorConstructor.Build(key))
            {
                var treeRebalancer = new TreeRebalancer(_llt, this, cursor);
                var changedPage = page;
                while (changedPage != null)
                {
                    changedPage = treeRebalancer.Execute(changedPage);
                }
            }

            page.DebugValidate(this, State.RootPageNumber);
        }

        public TreeIterator Iterate(bool prefetch)
        {
            return new TreeIterator(this, _llt, prefetch);
        }

        public ReadResult Read(Slice key)
        {
            TreeNodeHeader* node;
            var p = FindPageFor(key, out node);

            if (p.LastMatch != 0)
                return null;

            return new ReadResult(GetValueReaderFromHeader(node));
        }

        public int GetDataSize(Slice key)
        {
            TreeNodeHeader* node;
            var p = FindPageFor(key, out node);

            if (p.LastMatch != 0)
                return -1;

            if (node == null)
                return -1;

            Slice nodeKey;
            using (TreeNodeHeader.ToSlicePtr(_llt.Allocator, node, out nodeKey))
            {
                if (!SliceComparer.EqualsInline(nodeKey, key))
                    return -1;
            }

            return GetDataSize(node);
        }

        public int GetDataSize(TreeNodeHeader* node)
        {
            if (node->Flags == (TreeNodeFlags.PageRef))
            {
                var overFlowPage = GetReadOnlyPage(node->PageNumber);
                return overFlowPage.OverflowSize;
            }
            return node->DataSize;
        }

        public long GetParentPageOf(TreePage page)
        {
            Debug.Assert(page.IsCompressed == false);

            TreePage p;
            Slice key;

            using (page.IsLeaf ? page.GetNodeKey(_llt, 0, out key) : page.GetNodeKey(_llt, 1, out key))
            {
                TreeCursorConstructor cursorConstructor;
                TreeNodeHeader* node;
                p = FindPageFor(key, node: out node, cursor: out cursorConstructor, allowCompressed: true);

                if (p.LastMatch != 0)
                {
                    if (p.IsCompressed == false)
                        ThrowOnCompressedPage(p);
#if DEBUG
                    using (var decompressed = DecompressPage(p, skipCache: true))
                    {
                        decompressed.Search(_llt, key);
                        Debug.Assert(decompressed.LastMatch == 0);
                    }
#endif
                }

                using (var cursor = cursorConstructor.Build(key))
                {
                    while (cursor.PageCount > 0)
                    {
                        if (cursor.CurrentPage.PageNumber == page.PageNumber)
                        {
                            if (cursor.PageCount == 1)
                                return -1; // root page

                            return cursor.ParentPage.PageNumber;
                        }
                        cursor.Pop();
                    }
                }
            }

            return -1;
        }

        internal byte* DirectRead(Slice key)
        {
            TreeNodeHeader* node;
            var p = FindPageFor(key, out node);

            if (p == null || p.LastMatch != 0)
                return null;

            Debug.Assert(node != null);

            if (node->Flags == TreeNodeFlags.PageRef)
            {
                var overFlowPage = GetReadOnlyTreePage(node->PageNumber);
                return overFlowPage.Base + Constants.Tree.PageHeaderSize;
            }

            return (byte*)node + node->KeySize + Constants.Tree.NodeHeaderSize;
        }

        public List<long> AllPages()
        {
            var results = new List<long>();
            var stack = new Stack<TreePage>();
            var root = GetReadOnlyTreePage(State.RootPageNumber);
            stack.Push(root);

            Slice key = default(Slice);
            while (stack.Count > 0)
            {
                var p = stack.Pop();
                results.Add(p.PageNumber);

                for (int i = 0; i < p.NumberOfEntries; i++)
                {
                    var node = p.GetNode(i);
                    var pageNumber = node->PageNumber;
                    if (p.IsBranch)
                    {
                        stack.Push(GetReadOnlyTreePage(pageNumber));
                    }
                    else if (node->Flags == TreeNodeFlags.PageRef)
                    {
                        // This is an overflow page
                        var overflowPage = GetReadOnlyTreePage(pageNumber);
                        var numberOfPages = _llt.DataPager.GetNumberOfOverflowPages(overflowPage.OverflowSize);
                        for (long j = 0; j < numberOfPages; ++j)
                            results.Add(overflowPage.PageNumber + j);
                    }
                    else if (node->Flags == TreeNodeFlags.MultiValuePageRef)
                    {
                        using (TreeNodeHeader.ToSlicePtr(_tx.Allocator, node, out key))
                        {
                            var tree = OpenMultiValueTree(key, node);
                            results.AddRange(tree.AllPages());
                        }
                    }
                    else
                    {
                        if ((State.Flags & TreeFlags.FixedSizeTrees) == TreeFlags.FixedSizeTrees)
                        {
                            var valueReader = GetValueReaderFromHeader(node);
                            var valueSize = ((FixedSizeTreeHeader.Embedded*)valueReader.Base)->ValueSize;

                            Slice fixedSizeTreeName;
                            using (p.GetNodeKey(_llt, i, out fixedSizeTreeName))
                            {
                                var fixedSizeTree = new FixedSizeTree(_llt, this, fixedSizeTreeName, valueSize);

                                var pages = fixedSizeTree.AllPages();
                                results.AddRange(pages);
                            }
                        }
                    }
                }
            }
            return results;
        }

        public override string ToString()
        {
            return Name + " " + State.NumberOfEntries;
        }

        public void Dispose()
        {
            if (_fixedSizeTrees != null)
            {
                foreach (var tree in _fixedSizeTrees)
                {
                    tree.Value.Dispose();
                }
            }

            if (_pageLocator != null && _isPageLocatorOwned)
            {
                _llt.PersistentContext.FreePageLocator(_pageLocator);
                _pageLocator = null;
            }

            DecompressionsCache?.Dispose();
        }

        private bool TryOverwriteOverflowPages(TreeNodeHeader* updatedNode, int len, out byte* pos)
        {
            if (updatedNode->Flags == TreeNodeFlags.PageRef)
            {
                var readOnlyOverflowPage = GetReadOnlyTreePage(updatedNode->PageNumber);

                if (len <= readOnlyOverflowPage.OverflowSize)
                {
                    var availableOverflows = _llt.DataPager.GetNumberOfOverflowPages(readOnlyOverflowPage.OverflowSize);

                    var requestedOverflows = _llt.DataPager.GetNumberOfOverflowPages(len);

                    var overflowsToFree = availableOverflows - requestedOverflows;

                    for (int i = 0; i < overflowsToFree; i++)
                    {
                        _llt.FreePage(readOnlyOverflowPage.PageNumber + requestedOverflows + i);
                    }

                    State.RecordFreedPage(readOnlyOverflowPage, overflowsToFree);

                    var page = _llt.AllocatePage(requestedOverflows, updatedNode->PageNumber);
                    var writtableOverflowPage = PrepareTreePage(TreePageFlags.Value, requestedOverflows, page);

                    writtableOverflowPage.Flags = PageFlags.Overflow | PageFlags.VariableSizeTreePage;
                    writtableOverflowPage.OverflowSize = len;
                    pos = writtableOverflowPage.Base + Constants.Tree.PageHeaderSize;

                    PageModified?.Invoke(writtableOverflowPage.PageNumber, writtableOverflowPage.Flags);

                    return true;
                }
            }
            pos = null;
            return false;
        }

        public Slice LastKeyOrDefault()
        {
            using (var it = Iterate(false))
            {
                if (it.Seek(Slices.AfterAllKeys) == false)
                    return new Slice();

                return it.CurrentKey.Clone(_tx.Allocator);
            }
        }

        public Slice FirstKeyOrDefault()
        {
            using (var it = Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    return new Slice();

                return it.CurrentKey.Clone(_tx.Allocator);
            }
        }

        public void ClearPagesCache()
        {
            _recentlyFoundPages?.Clear();
        }

        public FixedSizeTree FixedTreeFor(Slice key, byte valSize = 0)
        {
            if (_fixedSizeTrees == null)
                _fixedSizeTrees = new Dictionary<Slice, FixedSizeTree>(SliceComparer.Instance);

            FixedSizeTree fixedTree;
            if (_fixedSizeTrees.TryGetValue(key, out fixedTree) == false)
            {
                fixedTree = new FixedSizeTree(_llt, this, key, valSize);
                _fixedSizeTrees[fixedTree.Name] = fixedTree;
            }

            State.Flags |= TreeFlags.FixedSizeTrees;

            return fixedTree;
        }

        public long DeleteFixedTreeFor(Slice key, byte valSize = 0)
        {
            var fixedSizeTree = FixedTreeFor(key, valSize);
            var numberOfEntries = fixedSizeTree.NumberOfEntries;

            foreach (var page in fixedSizeTree.AllPages())
            {
                if (_newPageAllocator != null)
                    _newPageAllocator.FreePage(page);
                else
                    _llt.FreePage(page);
            }
            _fixedSizeTrees.Remove(key);
            Delete(key);

            return numberOfEntries;
        }

        [Conditional("DEBUG")]
        public void DebugRenderAndShow()
        {
            DebugStuff.RenderAndShow(this);
        }

        public byte* DirectAccessFromHeader(TreeNodeHeader* node)
        {
            if (node->Flags == TreeNodeFlags.PageRef)
            {
                var overFlowPage = GetReadOnlyTreePage(node->PageNumber);
                return overFlowPage.Base + Constants.Tree.PageHeaderSize;
            }

            return (byte*)node + node->KeySize + Constants.Tree.NodeHeaderSize;
        }

        public Slice GetData(TreeNodeHeader* node)
        {
            Slice outputDataSlice;

            if (node->Flags == TreeNodeFlags.PageRef)
            {
                var overFlowPage = GetReadOnlyPage(node->PageNumber);
                if (overFlowPage.OverflowSize > ushort.MaxValue)
                    throw new InvalidOperationException("Cannot convert big data to a slice, too big");
                Slice.External(Llt.Allocator, overFlowPage.Pointer + Constants.Tree.PageHeaderSize,
                    (ushort)overFlowPage.OverflowSize, out outputDataSlice);
            }
            else
            {
                Slice.External(Llt.Allocator, (byte*)node + node->KeySize + Constants.Tree.NodeHeaderSize,
                    (ushort)node->DataSize, out outputDataSlice);
            }

            return outputDataSlice;
        }

        public ValueReader GetValueReaderFromHeader(TreeNodeHeader* node)
        {
            if (node->Flags == TreeNodeFlags.PageRef)
            {
                var overFlowPage = GetReadOnlyPage(node->PageNumber);

                Debug.Assert(overFlowPage.IsOverflow, "Requested overflow page but got " + overFlowPage.Flags);
                Debug.Assert(overFlowPage.OverflowSize > 0, "Overflow page cannot be size equal 0 bytes");

                return new ValueReader(overFlowPage.Pointer + Constants.Tree.PageHeaderSize, overFlowPage.OverflowSize);
            }
            return new ValueReader((byte*)node + node->KeySize + Constants.Tree.NodeHeaderSize, node->DataSize);
        }

        public void Rename(Slice newName)
        {
            Name = newName;
        }
    }
}
