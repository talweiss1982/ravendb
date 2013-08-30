// -----------------------------------------------------------------------
//  <copyright file="AsyncManualResetEvent.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Abstractions.Util
{
    public class AsyncManualResetEvent : IDisposable
    {
        private volatile TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>();
	    private bool disposed;

	    public Task WaitAsync() { return tcs.Task; }

        public async Task<bool> WaitAsync(int timeout)
        {
	        if (disposed)
		        throw new ObjectDisposedException("AsyncManualResetEvent");
            var task = tcs.Task;
#if !NET45
            return await TaskEx.WhenAny(task, TaskEx.Delay(timeout)) == task;

#else
            return await Task.WhenAny(task, Task.Delay(timeout)) == task;
#endif
        }

        public void Set()
        {
			if (disposed)
				throw new ObjectDisposedException("AsyncManualResetEvent"); 
				
			tcs.TrySetResult(true);
        }

        public void Reset()
        {
            while (true)
            {
				if (disposed)
					throw new ObjectDisposedException("AsyncManualResetEvent");

                var current = tcs;
                if (!current.Task.IsCompleted ||
                    Interlocked.CompareExchange(ref tcs, new TaskCompletionSource<bool>(), current) == current)
                    return;
            }
        }

	    public void Dispose()
	    {
			disposed = true;
			tcs.TrySetResult(true);
	    }
    }
}