using System;
using System.Threading;
using Sparrow.Logging;
using Voron.Impl;

namespace Raven.Server.ServerWide
{
    public class RavenTransaction : IDisposable
    {
        public Transaction InnerTransaction;
        private readonly Logger _logger;

        public RavenTransaction(Transaction transaction, Logger logger = null)
        {
            InnerTransaction = transaction;
            _logger = logger;
        }

        public void Commit()
        {
            InnerTransaction.Commit();
        }

        public void EndAsyncCommit()
        {
            InnerTransaction.EndAsyncCommit();
        }

        public bool Disposed;

        public virtual void Dispose()
        {
            if (Disposed)
                return;

            Disposed = true;

            var committed = InnerTransaction.LowLevelTransaction.Committed;

            InnerTransaction?.Dispose();
            InnerTransaction = null;

            if (committed)
                AfterCommit();
        }

        protected virtual void RaiseNotifications()
        {
        }

        protected virtual bool ShouldRaiseNotifications()
        {
            return false;
        }

        private void AfterCommit()
        {
            if (ShouldRaiseNotifications() == false)
                return;

            ThreadPool.QueueUserWorkItem(state =>
            {
                try
                {
                    ((RavenTransaction)state).RaiseNotifications();
                }
                catch (Exception e)
                {
                    if (_logger?.IsOperationsEnabled == true)
                        _logger.Operations("Failed to raise notifications", e);
                }
            }, this);
        }
    }
}
