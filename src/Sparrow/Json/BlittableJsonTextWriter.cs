﻿using System;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow.Extensions;

namespace Sparrow.Json
{
    public unsafe class BlittableJsonTextWriter : IDisposable
    {
        private readonly JsonOperationContext _context;
        private readonly Stream _stream;
        private const byte StartObject = (byte)'{';
        private const byte EndObject = (byte)'}';
        private const byte StartArray = (byte)'[';
        private const byte EndArray = (byte)']';
        private const byte Comma = (byte)',';
        private const byte Quote = (byte)'"';
        private const byte Colon = (byte)':';
        public static readonly byte[] NaNBuffer = { (byte)'"', (byte)'N', (byte)'a', (byte)'N', (byte)'"' };
        public static readonly byte[] PositiveInfinityBuffer =
        {
            (byte)'"', (byte)'I', (byte)'n', (byte)'f', (byte)'i', (byte)'n', (byte)'i', (byte)'t', (byte)'y', (byte)'"'
        };
        public static readonly byte[] NegativeInfinityBuffer =
        {
            (byte)'"', (byte)'-', (byte)'I', (byte)'n', (byte)'f', (byte)'i', (byte)'n', (byte)'i', (byte)'t', (byte)'y', (byte)'"'
        };
        public static readonly byte[] NullBuffer = { (byte)'n', (byte)'u', (byte)'l', (byte)'l', };
        public static readonly byte[] TrueBuffer = { (byte)'t', (byte)'r', (byte)'u', (byte)'e', };
        public static readonly byte[] FalseBuffer = { (byte)'f', (byte)'a', (byte)'l', (byte)'s', (byte)'e', };

        private int _pos;
        private readonly byte* _buffer;
        private readonly int _bufferLen;
        private JsonOperationContext.ReturnBuffer _returnBuffer;
        private readonly JsonOperationContext.ManagedPinnedBuffer _pinnedBuffer;
        private readonly AllocatedMemoryData _dateTimeMemory;

        public BlittableJsonTextWriter(JsonOperationContext context, Stream stream)
        {
            _context = context;
            _stream = stream;
            _returnBuffer = context.GetManagedBuffer(out _pinnedBuffer);
            _buffer = _pinnedBuffer.Pointer;
            _bufferLen = _pinnedBuffer.Length;
            _dateTimeMemory = context.GetMemory(32);
        }

        public int Position => _pos;

        public override string ToString()
        {
            return Encodings.Utf8.GetString(_pinnedBuffer.Buffer.Array, _pinnedBuffer.Buffer.Offset, _pos);
        }

        public void WriteObjectOrdered(BlittableJsonReaderObject obj)
        {
            WriteStartObject();
            var props = obj.GetPropertiesByInsertionOrder();
            var prop = new BlittableJsonReaderObject.PropertyDetails();
            for (int i = 0; i < props.Length; i++)
            {
                if (i != 0)
                {
                    WriteComma();
                }

                obj.GetPropertyByIndex(props[i], ref prop);
                WritePropertyName(prop.Name);

                WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value, originalPropertyOrder: true);
            }

            WriteEndObject();
        }

        public void WriteObject(BlittableJsonReaderObject obj)
        {
            if (obj == null)
            {
                WriteNull();
                return;
            }
            WriteStartObject();
            var prop = new BlittableJsonReaderObject.PropertyDetails();
            for (int i = 0; i < obj.Count; i++)
            {
                if (i != 0)
                {
                    WriteComma();
                }
                obj.GetPropertyByIndex(i, ref prop);
                WritePropertyName(prop.Name);

                WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value, originalPropertyOrder: false);
            }

            WriteEndObject();
        }


        private void WriteArrayToStream(BlittableJsonReaderArray blittableArray, bool originalPropertyOrder)
        {
            WriteStartArray();
            var length = blittableArray.Length;
            for (var i = 0; i < length; i++)
            {
                var propertyValueAndType = blittableArray.GetValueTokenTupleByIndex(i);

                if (i != 0)
                {
                    WriteComma();
                }
                // write field value
                WriteValue(propertyValueAndType.Item2, propertyValueAndType.Item1, originalPropertyOrder);

            }
            WriteEndArray();
        }

        public void WriteValue(BlittableJsonToken token, object val, bool originalPropertyOrder = false)
        {
            switch (token)
            {
                case BlittableJsonToken.String:
                    WriteString((LazyStringValue)val);
                    break;
                case BlittableJsonToken.Integer:
                    WriteInteger((long)val);
                    break;
                case BlittableJsonToken.StartArray:
                    WriteArrayToStream((BlittableJsonReaderArray)val, originalPropertyOrder);
                    break;
                case BlittableJsonToken.EmbeddedBlittable:
                case BlittableJsonToken.StartObject:
                    var blittableJsonReaderObject = ((BlittableJsonReaderObject)val);
                    if (originalPropertyOrder)
                        WriteObjectOrdered(blittableJsonReaderObject);
                    else
                        WriteObject(blittableJsonReaderObject);
                    break;
                case BlittableJsonToken.CompressedString:
                    WriteString((LazyCompressedStringValue)val);
                    break;
                case BlittableJsonToken.LazyNumber:
                    WriteDouble((LazyNumberValue)val);
                    break;
                case BlittableJsonToken.Boolean:
                    WriteBool((bool)val);
                    break;
                case BlittableJsonToken.Null:
                    WriteNull();
                    break;
                default:
                    throw new DataMisalignedException($"Unidentified Type {token}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteDateTime(DateTime value, bool isUtc)
        {
            int size = value.GetDefaultRavenFormat(_dateTimeMemory, isUtc);

            var strBuffer = _dateTimeMemory.Address;       

            WriteRawStringWhichMustBeWithoutEscapeChars(strBuffer, size);
        }

        public void WriteString(string str)
        {
            using (var lazyStr = _context.GetLazyString(str))
            {
                WriteString(lazyStr);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(LazyStringValue str)
        {
            if (str == null)
            {
                WriteNull();
                return;
            }

            var strBuffer = str.Buffer;
            var size = str.Size;

            EnsureBuffer(1);
            _buffer[_pos++] = Quote;
            var escapeSequencePos = size;
            var numberOfEscapeSequences = BlittableJsonReaderBase.ReadVariableSizeInt(str.Buffer, ref escapeSequencePos);
            if (numberOfEscapeSequences == 0)
            {
                WriteRawString(strBuffer, size);

                EnsureBuffer(1);
                _buffer[_pos++] = Quote;

                return;
            }

            UnlikelyWriteEscapeSequences(strBuffer, size, numberOfEscapeSequences, escapeSequencePos);
        }

        private void UnlikelyWriteEscapeSequences(byte* strBuffer, int size, int numberOfEscapeSequences, int escapeSequencePos)
        {
            var ptr = strBuffer;
            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;
                var bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(ptr, ref escapeSequencePos);
                WriteRawString(strBuffer, bytesToSkip);
                strBuffer += bytesToSkip;
                size -= bytesToSkip + 1 /*for the escaped char we skip*/;
                var b = *(strBuffer++);
                EnsureBuffer(2);
                _buffer[_pos++] = (byte) '\\';
                _buffer[_pos++] = GetEscapeCharacter(b);
            }
            // write remaining (or full string) to the buffer in one shot
            WriteRawString(strBuffer, size);

            EnsureBuffer(1);
            _buffer[_pos++] = Quote;
        }

        private byte GetEscapeCharacter(byte b)
        {
            switch (b)
            {
                case (byte)'\b':
                    return (byte)'b';
                case (byte)'\t':
                    return (byte)'t';
                case (byte)'\n':
                    return (byte)'n';
                case (byte)'\f':
                    return (byte)'f';
                case (byte)'\r':
                    return (byte)'r';
                case (byte)'\\':
                    return (byte)'\\';
                case (byte)'/':
                    return (byte)'/';
                case (byte)'"':
                    return (byte)'"';
                default:
                    throw new InvalidOperationException("Invalid escape char '" + (char)b + "' numeric value is: " + b);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(LazyCompressedStringValue str)
        {
            AllocatedMemoryData allocated;
            var strBuffer = str.DecompressToTempBuffer(out allocated);

            try
            {
                var size = str.UncompressedSize;

                EnsureBuffer(1);
                _buffer[_pos++] = Quote;
                var escapeSequencePos = str.CompressedSize;
                var numberOfEscapeSequences = BlittableJsonReaderBase.ReadVariableSizeInt(str.Buffer, ref escapeSequencePos);
                while (numberOfEscapeSequences > 0)
                {
                    numberOfEscapeSequences--;
                    var bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(str.Buffer, ref escapeSequencePos);
                    WriteRawString(strBuffer, bytesToSkip);
                    strBuffer += bytesToSkip;
                    size -= bytesToSkip + 1 /*for the escaped char we skip*/;
                    var b = *(strBuffer++);
                    EnsureBuffer(2);
                    _buffer[_pos++] = (byte)'\\';
                    _buffer[_pos++] = GetEscapeCharacter(b);
                }
                // write remaining (or full string) to the buffer in one shot
                WriteRawString(strBuffer, size);

                EnsureBuffer(1);
                _buffer[_pos++] = Quote;
            }
            finally
            {
                if(allocated != null) //precaution
                    _context.ReturnMemory(allocated);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteRawStringWhichMustBeWithoutEscapeChars(byte* buffer, int size)
        {
            EnsureBuffer(size + 2);
            _buffer[_pos++] = Quote;
            WriteRawString(buffer, size);            
            _buffer[_pos++] = Quote;
        }

        private void WriteRawString(byte* buffer, int size)
        {
            if (size < _bufferLen)
            {
                EnsureBuffer(size);
                Memory.Copy(_buffer + _pos, buffer, size);
                _pos += size;
                return;
            }

            UnlikelyWriteLargeRawString(buffer, size);
        }

        private void UnlikelyWriteLargeRawString(byte* buffer, int size)
        {
            // need to do this in pieces
            var posInStr = 0;
            while (posInStr < size)
            {
                var amountToCopy = Math.Min(size - posInStr, _bufferLen);
                Flush();
                Memory.Copy(_buffer, buffer + posInStr, amountToCopy);
                posInStr += amountToCopy;
                _pos = amountToCopy;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteStartObject()
        {
            EnsureBuffer(1);
            _buffer[_pos++] = StartObject;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteEndArray()
        {
            EnsureBuffer(1);
            _buffer[_pos++] = EndArray;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteStartArray()
        {
            EnsureBuffer(1);
            _buffer[_pos++] = StartArray;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteEndObject()
        {
            EnsureBuffer(1);
            _buffer[_pos++] = EndObject;
        }



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureBuffer(int len)
        {
            if (_pos + len < _bufferLen)
                return;
            if (len >= _bufferLen)
                ThrowValueTooBigForBuffer();

            Flush();
        }

        private static void ThrowValueTooBigForBuffer()
        {
            // ReSharper disable once NotResolvedInText
            throw new ArgumentOutOfRangeException("len");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Flush()
        {
            if (_stream == null)
                ThrowStreamClosed();
            if (_pos == 0)
                return;
            _stream.Write(_pinnedBuffer.Buffer.Array, _pinnedBuffer.Buffer.Offset, _pos);
            _pos = 0;
        }

        private void ThrowStreamClosed()
        {
            throw new ObjectDisposedException("The stream was closed already.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteNull()
        {
            EnsureBuffer(4);
            for (int i = 0; i < 4; i++)
            {
                _buffer[_pos++] = NullBuffer[i];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteBool(bool val)
        {
            EnsureBuffer(5);
            var buffer = val ? TrueBuffer : FalseBuffer;
            for (int i = 0; i < buffer.Length; i++)
            {
                _buffer[_pos++] = buffer[i];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteComma()
        {
            EnsureBuffer(1);
            _buffer[_pos++] = Comma;

        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WritePropertyName(LazyStringValue prop)
        {
            WriteString(prop);
            EnsureBuffer(1);
            _buffer[_pos++] = Colon;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WritePropertyName(string prop)
        {
            var lazyProp = _context.GetLazyStringForFieldWithCaching(prop);
            WriteString(lazyProp);
            EnsureBuffer(1);
            _buffer[_pos++] = Colon;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WritePropertyName(StringSegment prop)
        {
            var lazyProp = _context.GetLazyStringForFieldWithCaching(prop);
            WriteString(lazyProp);
            EnsureBuffer(1);
            _buffer[_pos++] = Colon;
        }

        public void WriteInteger(long val)
        {
            if (val == 0)
            {
                EnsureBuffer(1);
                _buffer[_pos++] = (byte)'0';
                return;
            }
            int len = 1;

            for (var i = val / 10; i != 0; i /= 10)
            {
                len++;
            }
            if (val < 0)
            {
                EnsureBuffer(len + 1);
                _buffer[_pos++] = (byte)'-';
            }
            else
            {
                EnsureBuffer(len);
            }
            for (int i = len - 1; i >= 0; i--)
            {
                _buffer[_pos + i] = (byte)('0' + Math.Abs(val % 10));
                val /= 10;
            }
            _pos += len;
        }

        public void WriteDouble(LazyNumberValue val)
        {
            if (val.IsNaN())
            {
                WriteBufferFor(NaNBuffer);
                return;
            }

            if (val.IsPositiveInfinity())
            {
                WriteBufferFor(PositiveInfinityBuffer);
                return;
            }

            if (val.IsNegativeInfinity())
            {
                WriteBufferFor(NegativeInfinityBuffer);
                return;
            }

            var lazyStringValue = val.Inner;
            WriteRawString(lazyStringValue.Buffer, lazyStringValue.Size);
        }

        public void WriteBufferFor(byte[] buffer)
        {
            EnsureBuffer(buffer.Length);
            for (int i = 0; i < buffer.Length; i++)
            {
                _buffer[_pos++] = buffer[i];
            }
        }

        public void WriteDouble(double val)
        {
            if (double.IsNaN(val))
            {
                WriteBufferFor(NaNBuffer);
                return;
            }

            if (double.IsPositiveInfinity(val))
            {
                WriteBufferFor(PositiveInfinityBuffer);
                return;
            }

            if (double.IsNegativeInfinity(val))
            {
                WriteBufferFor(NegativeInfinityBuffer);
                return;
            }

            using (var lazyStr = _context.GetLazyString(val.ToString(CultureInfo.InvariantCulture)))
            {
                WriteRawString(lazyStr.Buffer, lazyStr.Size);
            }
        }

        public void Dispose()
        {
            try
            {
                Flush();
            }
            catch (ObjectDisposedException)
            {
                //we are disposing, so this exception doesn't matter
            }
            finally
            {
                _returnBuffer.Dispose();
                _context.ReturnMemory(_dateTimeMemory);
            }
        }

        public void WriteNewLine()
        {
            EnsureBuffer(2);
            _buffer[_pos++] = (byte)'\r';
            _buffer[_pos++] = (byte)'\n';
        }

        public void WriteStream(Stream stream)
        {
            Flush();

            while (true)
            {
                _pos = stream.Read(_pinnedBuffer.Buffer.Array, _pinnedBuffer.Buffer.Offset, _pinnedBuffer.Buffer.Count);
                if (_pos == 0)
                    break;

                Flush();
            }
        }

        public void WriteMemoryChunk(IntPtr ptr, int size)
        {
            Flush();
            var p = (byte*)ptr.ToPointer();
            var leftToWrite = size;
            var totalWritten = 0;
            while (leftToWrite > 0)
            {
                var toWrite = Math.Min(_bufferLen, leftToWrite);
                Memory.Copy(_buffer, p + totalWritten, toWrite);
                _pos += toWrite;
                totalWritten += toWrite;
                leftToWrite -= toWrite;
                Flush();
            }
        }
    }
}
