﻿using System;
using System.Runtime.CompilerServices;
using Voron.Data;
using Voron.Global;

namespace Voron
{
    public readonly unsafe struct Page
    {
        public readonly byte* Pointer;
        
        public readonly Span<byte> AsSpan(int numberOfPages) => new Span<byte>(Pointer, Constants.Storage.PageSize * numberOfPages);
        
        public readonly Span<byte> AsSpan() => new Span<byte>(Pointer, Constants.Storage.PageSize);

        public Page(byte* pointer)
        {
            Pointer = pointer;
        }

        public bool IsValid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Pointer != null; }
        }

        public byte* DataPointer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Pointer + PageHeader.SizeOf; }
        }

        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return ((PageHeader*)Pointer)->PageNumber; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { ((PageHeader*)Pointer)->PageNumber = value; }
        }

        public bool IsOverflow
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (((PageHeader*)Pointer)->Flags & PageFlags.Overflow) == PageFlags.Overflow; }
        }

        public int OverflowSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return ((PageHeader*)Pointer)->OverflowSize; }
            set { ((PageHeader*)Pointer)->OverflowSize = value; }
        }

        public PageFlags Flags
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return ((PageHeader*)Pointer)->Flags; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { ((PageHeader*)Pointer)->Flags = value; }
        }
    }
}
