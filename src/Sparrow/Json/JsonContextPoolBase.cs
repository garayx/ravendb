﻿using System;
using System.Collections.Concurrent;
using Sparrow.Utils;

namespace Sparrow.Json
{
    public abstract class JsonContextPoolBase<T>
        where T : JsonOperationContext
    {
        private readonly ConcurrentStack<T> _contextPool;

        protected JsonContextPoolBase()
        {
            _contextPool = new ConcurrentStack<T>();
        }

        public IDisposable AllocateOperationContext(out JsonOperationContext context)
        {
            T ctx;
            var disposable = AllocateOperationContext(out ctx);
            context = ctx;

            return disposable;
        }

        public IDisposable AllocateOperationContext(out T context)
        {
            if (_contextPool.TryPop(out context) == false)
                context = CreateContext();

            return new ReturnRequestContext
            {
                Parent = this,
                Context = context
            };
        }

        protected abstract T CreateContext();

        private class ReturnRequestContext : IDisposable
        {
            public T Context;
            public JsonContextPoolBase<T> Parent;
            public void Dispose()
            {
                if (Parent._contextPool.Count < 4096)
                {
                    Context.Reset();
                    Parent._contextPool.Push(Context);
                }
                else
                {
                    Context.Dispose();
                }
            }
        }

        public void Dispose()
        {
            T result;
            while (_contextPool.TryPop(out result))
            {
                result.Dispose();
            }
        }
    }
}