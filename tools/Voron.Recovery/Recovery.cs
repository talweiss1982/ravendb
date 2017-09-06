﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.Json;
using Raven.Server.Documents;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Revisions;
using Raven.Server.Smuggler.Documents;
using Sparrow;
using Sparrow.Json;
using Sparrow.LowMemory;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.RawData;
using Voron.Data.Tables;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;
using static System.String;

namespace Voron.Recovery
{
    public unsafe class Recovery
    {
        public Recovery(VoronRecoveryConfiguration config)
        {
            _datafile = config.PathToDataFile;
            _output = config.OutputFileName;
            _pageSize = config.PageSizeInKb * Constants.Size.Kilobyte;
            _initialContextSize = config.InitialContextSizeInMB * Constants.Size.Megabyte;
            _initialContextLongLivedSize = config.InitialContextLongLivedSizeInKB * Constants.Size.Kilobyte;
            _option = StorageEnvironmentOptions.ForPath(config.DataFileDirectory, null, Path.Combine(config.DataFileDirectory, "Journal"), null, null);
            _copyOnWrite = !config.DisableCopyOnWriteMode;
            // by default CopyOnWriteMode will be true
            _option.CopyOnWriteMode = _copyOnWrite;
            _progressIntervalInSec = config.ProgressIntervalInSec;
        }


        private readonly byte[] _streamHashState = new byte[Sodium.crypto_generichash_statebytes()];
        private readonly byte[] _streamHashResult = new byte[(int)Sodium.crypto_generichash_bytes()];
        private List<(IntPtr Ptr, int Size)> _attachmentChunks = new List<(IntPtr Ptr, int Size)>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long GetFilePosition(long offset, byte* position)
        {
            return (long)position - offset;
        }

        public RecoveryStatus Execute(CancellationToken ct)
        {
            var sw = new Stopwatch();
            StorageEnvironment se = null;
            sw.Start();
            if (_copyOnWrite)
            {
                Console.WriteLine("Recovering journal files, this may take a while...");
                try
                {

                    se = new StorageEnvironment(_option);
                    Console.WriteLine(
                        $"Journal recovery has completed successfully within {sw.Elapsed.TotalSeconds:N1} seconds");
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Journal recovery failed, reason:{Environment.NewLine}{e}");
                }
                finally
                {
                    se?.Dispose();
                }
            }
            _option = StorageEnvironmentOptions.ForPath(Path.GetDirectoryName(_datafile));

            var mem = Pager.AcquirePagePointer(null, 0);
            long startOffset = (long)mem;
            var fi = new FileInfo(_datafile);
            var fileSize = fi.Length;
            //making sure eof is page aligned
            var eof = mem + (fileSize / _pageSize) * _pageSize;
            DateTime lastProgressReport = DateTime.MinValue;
            using (var destinationStreamAttachments = File.OpenWrite(Path.Combine(Path.GetDirectoryName(_output), Path.GetFileNameWithoutExtension(_output) + "-1-Attachments" + Path.GetExtension(_output))))
            using (var destinationStreamDocuments = File.OpenWrite(Path.Combine(Path.GetDirectoryName(_output), Path.GetFileNameWithoutExtension(_output) + "-2-Documents" + Path.GetExtension(_output))))
            using (var destinationStreamRevisions = File.OpenWrite(Path.Combine(Path.GetDirectoryName(_output), Path.GetFileNameWithoutExtension(_output) + "-3-Revisions" + Path.GetExtension(_output))))
            using (var destinationStreamConflicts = File.OpenWrite(Path.Combine(Path.GetDirectoryName(_output), Path.GetFileNameWithoutExtension(_output) + "-4-Conflicts" + Path.GetExtension(_output))))
            using (var logFile = File.CreateText(Path.Combine(Path.GetDirectoryName(_output), LogFileName)))
            using (var gZipStreamDocuments = new GZipStream(destinationStreamDocuments, CompressionMode.Compress, true))
            using (var gZipStreamRevisions = new GZipStream(destinationStreamRevisions, CompressionMode.Compress, true))
            using (var gZipStreamConflicts = new GZipStream(destinationStreamConflicts, CompressionMode.Compress, true))
            using (var gZipStreamAttachments = new GZipStream(destinationStreamAttachments, CompressionMode.Compress, true))
            using (var context = new JsonOperationContext(_initialContextSize, _initialContextLongLivedSize, SharedMultipleUseFlag.None))
            using (var documentsWriter = new BlittableJsonTextWriter(context, gZipStreamDocuments))
            using (var revisionsWriter = new BlittableJsonTextWriter(context, gZipStreamRevisions))
            using (var conflictsWriter = new BlittableJsonTextWriter(context, gZipStreamConflicts))
            using (var attachmentWriter = new BlittableJsonTextWriter(context, gZipStreamAttachments))
            {
                WriteSmugglerHeader(attachmentWriter, 40018, "Docs");
                WriteSmugglerHeader(documentsWriter, 40018, "Docs");
                WriteSmugglerHeader(revisionsWriter, 40018, "RevisionDocuments");
                WriteSmugglerHeader(conflictsWriter, 40018, "ConflictDocuments");

                while (mem < eof)
                {
                    try
                    {
                        if (ct.IsCancellationRequested)
                        {
                            logFile.WriteLine(
                                $"Cancellation requested while recovery was in position {GetFilePosition(startOffset, mem)}");
                            _cancellationRequested = true;
                            break;
                        }
                        var now = DateTime.UtcNow;
                        if ((now - lastProgressReport).TotalSeconds >= _progressIntervalInSec)
                        {
                            if (lastProgressReport != DateTime.MinValue)
                            {
                                Console.Clear();
                                Console.WriteLine("Press 'q' to quit the recovery process");
                            }
                            lastProgressReport = now;
                            var currPos = GetFilePosition(startOffset, mem);
                            var eofPos = GetFilePosition(startOffset, eof);
                            Console.WriteLine(
                                $"{now:hh:MM:ss}: Recovering page at position {currPos:#,#;;0}/{eofPos:#,#;;0} ({(double)currPos / eofPos:p}) - Last recovered doc is {_lastRecoveredDocumentKey}");
                        }

                        var pageHeader = (PageHeader*)mem;

                        //this page is not raw data section move on
                        if ((pageHeader->Flags).HasFlag(PageFlags.RawData) == false && pageHeader->Flags.HasFlag(PageFlags.Stream) == false)
                        {
                            mem += _pageSize;
                            continue;
                        }

                        if (pageHeader->Flags.HasFlag(PageFlags.Single) &&
                            pageHeader->Flags.HasFlag(PageFlags.Overflow))
                        {
                            var message =
                                $"page #{pageHeader->PageNumber} (offset={GetFilePosition(startOffset, mem)}) has both Overflow and Single flag turned";
                            mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                            continue;
                        }
                        //overflow page
                        ulong checksum;
                        if (pageHeader->Flags.HasFlag(PageFlags.Overflow))
                        {
                            if (ValidateOverflowPage(pageHeader, eof, startOffset, logFile, ref mem) == false)
                                continue;

                            var numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(pageHeader->OverflowSize);

                            if (pageHeader->Flags.HasFlag(PageFlags.Stream))
                            {
                                var streamPageHeader = (StreamPageHeader*)pageHeader;
                                if (streamPageHeader->StreamPageFlags.HasFlag(StreamPageFlags.First) == false)
                                {
                                    mem += numberOfPages * _pageSize;
                                    continue;
                                }

                                int rc;
                                fixed (byte* hashStatePtr = _streamHashState)
                                fixed (byte* hashResultPtr = _streamHashResult)
                                {
                                    long totalSize = 0;
                                    _attachmentChunks.Clear();
                                    rc = Sodium.crypto_generichash_init(hashStatePtr, null, UIntPtr.Zero, (UIntPtr)_streamHashResult.Length);
                                    if (rc != 0)
                                    {
                                        logFile.WriteLine($"page #{pageHeader->PageNumber} (offset={(long)pageHeader}) failed to initialize Sodium for hash computation will skip this page.");
                                        mem += numberOfPages * _pageSize;
                                        continue;
                                    }
                                    // write document header, including size
                                    PageHeader* nextPage = pageHeader;
                                    byte* nextPagePtr = (byte*)nextPage;
                                    bool valid = true;
                                    while (true) // has next
                                    {
                                        streamPageHeader = (StreamPageHeader*)nextPage;
                                        //this is the last page and it contains only stream info + maybe the stream tag
                                        if (streamPageHeader->ChunkSize == 0)
                                            break;
                                        totalSize += streamPageHeader->ChunkSize;                                        
                                        var dataStart = (byte*)nextPage + PageHeader.SizeOf;
                                        _attachmentChunks.Add(((IntPtr)dataStart, (int)streamPageHeader->ChunkSize));
                                        rc = Sodium.crypto_generichash_update(hashStatePtr, dataStart, (ulong)streamPageHeader->ChunkSize);
                                        if (rc != 0)
                                        {
                                            logFile.WriteLine($"page #{pageHeader->PageNumber} (offset={(long)pageHeader}) failed to compute chunk hash, will skip it.");
                                            valid = false;
                                            break;
                                        }
                                        if (streamPageHeader->StreamNextPageNumber == 0 )
                                            break;
                                        nextPage = (PageHeader*)(streamPageHeader->StreamNextPageNumber * _pageSize + startOffset);
                                        //This is the case that the next page isn't a stream page
                                        if (nextPage->Flags.HasFlag(PageFlags.Stream) == false || nextPage->Flags.HasFlag(PageFlags.Overflow) == false)
                                        {
                                            valid = false;
                                            logFile.WriteLine($"page #{nextPage->PageNumber} (offset={(long)nextPage}) was suppose to be a stream chunck but isn't marked as Overflow | Stream");
                                            break;
                                        }
                                        valid = ValidateOverflowPage(nextPage, eof, (long)nextPage, logFile, ref nextPagePtr);
                                        if (valid == false)
                                        {
                                            break;
                                        }                                        

                                    }
                                    if (valid == false)
                                    {
                                        //The first page was valid so we can skip the entire overflow
                                        mem += numberOfPages * _pageSize;
                                        continue;
                                    }

                                    rc = Sodium.crypto_generichash_final(hashStatePtr, hashResultPtr, (UIntPtr)_streamHashResult.Length);
                                    if (rc != 0)
                                    {
                                        logFile.WriteLine($"page #{pageHeader->PageNumber} (offset={(long)pageHeader}) failed to compute attachment hash, will skip it.");
                                        mem += numberOfPages * _pageSize;
                                        continue;
                                    }
                                    var hash = new string(' ', 44);
                                    fixed (char* p = hash)
                                    {
                                        var len = Base64.ConvertToBase64Array(p, hashResultPtr, 0, 32);
                                        Debug.Assert(len == 44);
                                    }

                                    WriteAttachment(attachmentWriter, totalSize, hash);
                                }
                                mem += numberOfPages * _pageSize;
                            }

                            else if (Write((byte*)pageHeader + PageHeader.SizeOf, pageHeader->OverflowSize, documentsWriter, revisionsWriter,
                                conflictsWriter, logFile, context, startOffset, ((RawDataOverflowPageHeader*)mem)->TableType))
                            {

                                mem += numberOfPages * _pageSize;
                            }
                            else //write document failed 
                            {
                                mem += _pageSize;
                            }
                            continue;
                        }

                        checksum = StorageEnvironment.CalculatePageChecksum((byte*)pageHeader, pageHeader->PageNumber, pageHeader->Flags, 0);

                        if (checksum != pageHeader->Checksum)
                        {
                            var message =
                                $"Invalid checksum for page {pageHeader->PageNumber}, expected hash to be {pageHeader->Checksum} but was {checksum}";
                            mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                            continue;
                        }

                        // small raw data section 
                        var rawHeader = (RawDataSmallPageHeader*)mem;

                        // small raw data section header
                        if (rawHeader->RawDataFlags.HasFlag(RawDataPageFlags.Header))
                        {
                            mem += _pageSize;
                            continue;
                        }
                        if (rawHeader->NextAllocation > _pageSize)
                        {
                            var message =
                                $"RawDataSmallPage #{rawHeader->PageNumber} at {GetFilePosition(startOffset, mem)} next allocation is larger than {_pageSize} bytes";
                            mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                            continue;
                        }

                        for (var pos = PageHeader.SizeOf; pos < rawHeader->NextAllocation;)
                        {
                            var currMem = mem + pos;
                            var entry = (RawDataSection.RawDataEntrySizes*)currMem;
                            //this indicates that the current entry is invalid because it is outside the size of a page
                            if (pos > _pageSize)
                            {
                                var message =
                                    $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffset, currMem)}";
                                mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                                //we can't retrive entries past the invalid entry
                                break;
                            }
                            //Allocated size of entry exceed the bound of the page next allocation
                            if (entry->AllocatedSize + pos + sizeof(RawDataSection.RawDataEntrySizes) >
                                rawHeader->NextAllocation)
                            {
                                var message =
                                    $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffset, currMem)}" +
                                    "the allocated entry exceed the bound of the page next allocation.";
                                mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                                //we can't retrive entries past the invalid entry
                                break;
                            }
                            if (entry->UsedSize > entry->AllocatedSize)
                            {
                                var message =
                                    $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffset, currMem)}" +
                                    "the size of the entry exceed the allocated size";
                                mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                                //we can't retrive entries past the invalid entry
                                break;
                            }
                            pos += entry->AllocatedSize + sizeof(RawDataSection.RawDataEntrySizes);
                            if (entry->AllocatedSize == 0 || entry->UsedSize == -1)
                                continue;

                            if (Write(currMem + sizeof(RawDataSection.RawDataEntrySizes), entry->UsedSize, documentsWriter, revisionsWriter,
                                conflictsWriter, logFile, context, startOffset, ((RawDataSmallPageHeader*)mem)->TableType) == false)
                                break;
                        }
                        mem += _pageSize;
                    }
                    catch (Exception e)
                    {
                        var message =
                            $"Unexpected exception at position {GetFilePosition(startOffset, mem)}:{Environment.NewLine} {e}";
                        mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                    }
                }
                attachmentWriter.WriteEndArray();
                documentsWriter.WriteEndArray();
                conflictsWriter.WriteEndArray();
                revisionsWriter.WriteEndArray();
                attachmentWriter.WriteEndObject();
                documentsWriter.WriteEndObject();
                conflictsWriter.WriteEndObject();
                revisionsWriter.WriteEndObject();

                ReportOrphanAttachmentsAndMissingAttachments(logFile, ct);
                logFile.WriteLine(
                    $"Discovered a total of {_numberOfDocumentsRetrieved:#,#;00} documents within {sw.Elapsed.TotalSeconds::#,#.#;;00} seconds.");
                logFile.WriteLine($"Discovered a total of {_attachmentsHashs.Count:#,#;00} attachments.");
                logFile.WriteLine($"Discovered a total of {_numberOfFaultedPages::#,#;00} faulted pages.");
            }
            if (_cancellationRequested)
                return RecoveryStatus.CancellationRequested;
            return RecoveryStatus.Success;
        }

        private void ReportOrphanAttachmentsAndMissingAttachments(StreamWriter logFile, CancellationToken ct)
        {
            //No need to scare the user if there are no attachments in the dump
            if (_attachmentsHashs.Count == 0 && _documentsAttachments.Count == 0)
                return;
            if (_attachmentsHashs.Count == 0)
            {
                logFile.WriteLine("No attachments were recoverd but there are documents pointing to attachments.");
                return;
            }
            if (_documentsAttachments.Count == 0)
            {
                foreach (var hash in _attachmentsHashs)
                {
                    logFile.WriteLine($"Found orphan attachment with hash {hash}.");
                }
                return;
            }
            Console.WriteLine("Starting to compute orphan and missing attachments this may take a while.");
            if (ct.IsCancellationRequested)
            {
                return;
            }
            _attachmentsHashs.Sort();
            if (ct.IsCancellationRequested)
            {
                return;
            }
            _documentsAttachments.Sort((x,y)=>Compare(x.hash, y.hash, StringComparison.Ordinal));
            //We rely on the fact that the attachment hash are unqiue in the _attachmentsHashs list (no duplicated values).
            int index = 0;
            foreach (var hash in _attachmentsHashs)
            {
                if (ct.IsCancellationRequested)
                {
                    return;
                }
                var foundEqual = false;
                while (_documentsAttachments.Count > index)
                {
                    var documentHash = _documentsAttachments[index].hash;
                    var compareResult = Compare(hash, documentHash, StringComparison.Ordinal);
                    if (compareResult == 0)
                    {
                        index++;
                        foundEqual = true;
                        continue;
                    }
                    //this is the case where we have a document with a hash but no attachment with that hash
                    if (compareResult > 0)
                    {
                        logFile.WriteLine(
                            $"Document {_documentsAttachments[index].docId} contians atachment with hash {documentHash} but we were not able to recover such attachment.");
                        index++;
                        continue;
                    }
                    break;
                }
                if (foundEqual == false)
                {
                    logFile.WriteLine($"Found orphan attachment with hash {hash}.");
                }

            }
        }

        private bool _firstAttachment = true;
        private long _attachmentNumber = 0;
        private List<string> _attachmentsHashs = new List<string>();
        private void WriteAttachment(BlittableJsonTextWriter attachmentWriter, long totalSize, string hash)
        {
            if (_firstAttachment == false)
            {
                attachmentWriter.WriteComma();
            }
            _firstAttachment = false;

            attachmentWriter.WriteStartObject();

            attachmentWriter.WritePropertyName(DocumentItem.Key);
            attachmentWriter.WriteInteger((byte)DocumentType.Attachment);
            attachmentWriter.WriteComma();

            attachmentWriter.WritePropertyName(nameof(AttachmentName.Hash));
            attachmentWriter.WriteString(hash);
            attachmentWriter.WriteComma();

            attachmentWriter.WritePropertyName(nameof(AttachmentName.Size));
            attachmentWriter.WriteInteger(totalSize);
            attachmentWriter.WriteComma();

            attachmentWriter.WritePropertyName(nameof(DocumentItem.AttachmentStream.Tag));
            attachmentWriter.WriteString($"Recovered attachment #{++_attachmentNumber}");

            attachmentWriter.WriteEndObject();
            foreach (var chunk in _attachmentChunks)
            {
                attachmentWriter.WriteMemoryChunk(chunk.Ptr,chunk.Size);
            }
            _attachmentsHashs.Add(hash);
        }


        private bool ValidateOverflowPage(PageHeader* pageHeader, byte* eof, long startOffset, StreamWriter logFile, ref byte* mem)
        {
            ulong checksum;
            var endOfOverflow = (byte*)pageHeader + VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(pageHeader->OverflowSize) * _pageSize;
            // the endOfOeverFlow can be equal to eof if the last page is overflow
            if (endOfOverflow > eof)
            {
                var message =
                    $"Overflow page #{pageHeader->PageNumber} (offset={GetFilePosition(startOffset, mem)})" +
                    $" size exceeds the end of the file ([{(long)pageHeader}:{(long)endOfOverflow}])";
                mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                return false;
            }

            if (pageHeader->OverflowSize <= 0)
            {
                var message =
                    $"Overflow page #{pageHeader->PageNumber} (offset={GetFilePosition(startOffset, mem)})" +
                    $" OverflowSize is not a positive number ({pageHeader->OverflowSize})";
                mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                return false;
            }
            // this can only be here if we know that the overflow size is valid
            checksum = StorageEnvironment.CalculatePageChecksum((byte*)pageHeader, pageHeader->PageNumber, pageHeader->Flags, pageHeader->OverflowSize);

            if (checksum != pageHeader->Checksum)
            {
                var message =
                    $"Invalid checksum for overflow page {pageHeader->PageNumber}, expected hash to be {pageHeader->Checksum} but was {checksum}";
                mem = PrintErrorAndAdvanceMem(message, mem, logFile);
                return false;
            }
            return true;
        }

        private void WriteSmugglerHeader(BlittableJsonTextWriter writer, int version,string docType)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(("BuildVersion"));
            writer.WriteInteger(version);
            writer.WriteComma();
            writer.WritePropertyName(docType);
            writer.WriteStartArray();
        }

        private bool Write(byte* mem, int sizeInBytes, BlittableJsonTextWriter documentsWriter, BlittableJsonTextWriter revisionsWriter, 
            BlittableJsonTextWriter conflictsWritet, StreamWriter logWriter, JsonOperationContext context, long startOffest, byte tableType)
        {
            switch ((TableType)tableType)
            {
                case TableType.None:
                    return false;
                case TableType.Documents:
                    return WriteDocument(mem, sizeInBytes, documentsWriter, logWriter, context, startOffest);
                case TableType.Revisions:
                    return WriteRevision(mem, sizeInBytes, revisionsWriter, logWriter, context, startOffest);
                case TableType.Conflicts:
                    return WriteConflict(mem, sizeInBytes, conflictsWritet, logWriter, context, startOffest);
                default:
                    throw new ArgumentOutOfRangeException(nameof(tableType), tableType, null);
            }
        }

        private bool WriteDocument(byte* mem, int sizeInBytes, BlittableJsonTextWriter writer, StreamWriter logWriter, JsonOperationContext context, long startOffest)
        {
            try
            {
                var tvr = new TableValueReader(mem, sizeInBytes);

                if (_documentWritten)
                    writer.WriteComma();

                _documentWritten = false;

                Document document = null;
                try
                {
                    document = DocumentsStorage.ParseRawDataSectionDocumentWithValidation(context, ref tvr, sizeInBytes);
                    if (document == null)
                    {
                        logWriter.WriteLine($"Failed to convert table value to document at position {GetFilePosition(startOffest, mem)}");
                        return false;
                    }
                    document.EnsureMetadata();
                    document.Data.BlittableValidation();
                }
                catch (Exception e)
                {
                    logWriter.WriteLine(
                        $"Found invalid blittable document at pos={GetFilePosition(startOffest, mem)} with key={document?.Id ?? "null"}{Environment.NewLine}{e}");
                    return false;
                }

                context.Write(writer, document.Data);

                _documentWritten = true;
                _numberOfDocumentsRetrieved++;
                logWriter.WriteLine($"Found document with key={document.Id}");
                _lastRecoveredDocumentKey = document.Id;

                HandleDocumentAttachments(logWriter, document);
                
                return true;
            }
            catch (Exception e)
            {
                logWriter.WriteLine($"Unexpected exception while writing document at position {GetFilePosition(startOffest, mem)}: {e}");
                return false;
            }
        }

        private void HandleDocumentAttachments(StreamWriter logWriter, Document document)
        {
            if (document.Flags.HasFlag(DocumentFlags.HasAttachments))
            {
                var metadata = document.Data.GetMetadata();
                if (metadata == null)
                {
                    logWriter.WriteLine(
                        $"Document {document.Id} has attachment flag set but was unable to read its metadata and retrieve the attachments hashes");
                    return;
                }
                var metadataDictionary = new MetadataAsDictionary(metadata);
                var attachments = metadataDictionary.GetObjects(Raven.Client.Constants.Documents.Metadata.Attachments);
                foreach (var attachment in attachments)
                {
                    var hash = attachment.GetString(nameof(AttachmentName.Hash));
                    if (IsNullOrEmpty(hash))
                        continue;
                    _documentsAttachments.Add((hash, document.Id));
                }
            }
        }

        private bool WriteRevision(byte* mem, int sizeInBytes, BlittableJsonTextWriter writer, StreamWriter logWriter, JsonOperationContext context, long startOffest)
        {
            try
            {
                var tvr = new TableValueReader(mem, sizeInBytes);

                if (_revisionWritten)
                    writer.WriteComma();

                _revisionWritten = false;

                Document revision = null;
                try
                {
                    revision = RevisionsStorage.ParseRawDataSectionRevisionWithValidation(context, ref tvr, sizeInBytes);
                    if (revision == null)
                    {
                        logWriter.WriteLine($"Failed to convert table value to revision document at position {GetFilePosition(startOffest, mem)}");
                        return false;
                    }
                    revision.EnsureMetadata();
                    revision.Data.BlittableValidation();
                }
                catch (Exception e)
                {
                    logWriter.WriteLine(
                        $"Found invalid blittable revision document at pos={GetFilePosition(startOffest, mem)} with key={revision?.Id ?? "null"}{Environment.NewLine}{e}");
                    return false;
                }

                context.Write(writer, revision.Data);

                _revisionWritten = true;
                _numberOfDocumentsRetrieved++;
                logWriter.WriteLine($"Found revision document with key={revision.Id}");
                _lastRecoveredDocumentKey = revision.Id;
                return true;
            }
            catch (Exception e)
            {
                logWriter.WriteLine($"Unexpected exception while writing revision document at position {GetFilePosition(startOffest, mem)}: {e}");
                return false;
            }
        }

        private bool WriteConflict(byte* mem, int sizeInBytes, BlittableJsonTextWriter writer, StreamWriter logWriter, JsonOperationContext context, long startOffest)
        {
            try
            {
                var tvr = new TableValueReader(mem, sizeInBytes);

                if (_conflictWritten)
                    writer.WriteComma();

                _conflictWritten = false;

                DocumentConflict conflict = null;
                try
                {
                    conflict = ConflictsStorage.ParseRawDataSectionConflictWithValidation(context, ref tvr, sizeInBytes);
                    if (conflict == null)
                    {
                        logWriter.WriteLine($"Failed to convert table value to conflict document at position {GetFilePosition(startOffest, mem)}");
                        return false;
                    }
                    conflict.Doc.BlittableValidation();
                }
                catch (Exception e)
                {
                    logWriter.WriteLine(
                        $"Found invalid blittable conflict document at pos={GetFilePosition(startOffest, mem)} with key={conflict?.Id ?? "null"}{Environment.NewLine}{e}");
                    return false;
                }

                context.Write(writer, conflict.Doc);

                _conflictWritten = true;
                _numberOfDocumentsRetrieved++;
                logWriter.WriteLine($"Found conflict document with key={conflict.Id}");
                _lastRecoveredDocumentKey = conflict.Id;
                return true;
            }
            catch (Exception e)
            {
                logWriter.WriteLine($"Unexpected exception while writing conflict document at position {GetFilePosition(startOffest, mem)}: {e}");
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte* PrintErrorAndAdvanceMem(string message, byte* mem, StreamWriter writer)
        {
            writer.WriteLine(message);
            _numberOfFaultedPages++;
            return mem + _pageSize;
        }

        private readonly string _output;
        private readonly int _pageSize;
        private AbstractPager Pager => _option.DataPager;
        private const string LogFileName = "recovery.log";
        private long _numberOfFaultedPages;
        private long _numberOfDocumentsRetrieved;
        private readonly int _initialContextSize;
        private readonly int _initialContextLongLivedSize;
        private bool _documentWritten;
        private bool _revisionWritten;
        private bool _conflictWritten;
        private StorageEnvironmentOptions _option;
        private readonly int _progressIntervalInSec;
        private bool _cancellationRequested;
        private string _lastRecoveredDocumentKey = "No documents recovered yet";
        private readonly string _datafile;
        private readonly bool _copyOnWrite;
        private readonly List<(string hash,string docId)> _documentsAttachments = new List<(string hash, string docId)>();

        public enum RecoveryStatus
        {
            Success,
            CancellationRequested
        }
    }
}
