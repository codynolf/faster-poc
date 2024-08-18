using faster.lib.options;
using FASTER.core;
using Microsoft.Extensions.Options;
namespace faster.lib;

public class FasterKvService<K, V> : IFasterKvService<K, V>
{
    readonly FasterKV<K, V> _store;
    readonly AsyncPool<ClientSession<K, V, V, V, Empty, SimpleFunctions<K, V, Empty>>> _sessionPool;

    public FasterKvService(IOptions<FasterKvOptions> fasterKvOptions)
    {
        var logSettings = new LogSettings
        {
            LogDevice = new ManagedLocalStorageDevice(
                filename: Path.Combine(fasterKvOptions.Value.LogDirectory, $"{fasterKvOptions.Value.LogFileName}.log"), 
                deleteOnClose: fasterKvOptions.Value.DeleteOnClose, 
                osReadBuffering: fasterKvOptions.Value.UseOsReadBuffering),
            ObjectLogDevice = new ManagedLocalStorageDevice(
                filename: Path.Combine(fasterKvOptions.Value.LogDirectory, $"{fasterKvOptions.Value.LogFileName}.obj.log"), 
                deleteOnClose: fasterKvOptions.Value.DeleteOnClose, 
                osReadBuffering: fasterKvOptions.Value.UseOsReadBuffering)
        };

        if (!fasterKvOptions.Value.UseLargeLog)
        {
            logSettings.PageSizeBits = 12;
            logSettings.MemorySizeBits = 13;
        }

        _store = new FasterKV<K, V>(
            size: fasterKvOptions.Value.InitialSize, 
            logSettings: logSettings,
            tryRecoverLatest: fasterKvOptions.Value.TryRecoverLatest);
        _sessionPool = new AsyncPool<ClientSession<K, V, V, V, Empty, SimpleFunctions<K, V, Empty>>>(
                logSettings.LogDevice.ThrottleLimit,
                () => _store.For(new SimpleFunctions<K, V, Empty>()).NewSession<SimpleFunctions<K, V, Empty>>());
    }

    async ValueTask<int> CompleteAsync(FasterKV<K, V>.UpsertAsyncResult<V, V, Empty> result)
    {
        var numPending = 0;
        for (; result.Status.IsPending; ++numPending)
            result = await result.CompleteAsync().ConfigureAwait(false);
        return numPending;
    }

    public async ValueTask UpsertAsync(K key, V value)
    {
        if (!_sessionPool.TryGet(out var session))
            session = await _sessionPool.GetAsync().ConfigureAwait(false);
        await CompleteAsync(await session.UpsertAsync(key, value).ConfigureAwait(false));
        _sessionPool.Return(session);
    }

    public void Upsert(K key, V value)
    {
        if (!_sessionPool.TryGet(out var session))
            session = _sessionPool.GetAsync().GetAwaiter().GetResult();
        var status = session.Upsert(key, value);
        _sessionPool.Return(session);
    }

    public async ValueTask RMWAsync(K key, V value)
    {
        if (!_sessionPool.TryGet(out var session))
            session = await _sessionPool.GetAsync().ConfigureAwait(false);
        _sessionPool.Return(session);
    }

    public void RMW(K key, V value)
    {
        if (!_sessionPool.TryGet(out var session))
            session = _sessionPool.GetAsync().GetAwaiter().GetResult();
        var status = session.RMW(key, value);
        _sessionPool.Return(session);
    }

    public async ValueTask<(Status, V)> ReadAsync(K key)
    {
        if (!_sessionPool.TryGet(out var session))
            session = await _sessionPool.GetAsync().ConfigureAwait(false);
        var result = (await session.ReadAsync(key).ConfigureAwait(false)).Complete();
        _sessionPool.Return(session);
        return result;
    }

    public V Read(K key)
    {
        if (!_sessionPool.TryGet(out var session))
            session = _sessionPool.GetAsync().GetAwaiter().GetResult();
        var result = session.Read(key);
        if (result.status.IsPending)
        {
            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            int count = 0;
            for (; completedOutputs.Next(); ++count)
            {
                result = (completedOutputs.Current.Status, completedOutputs.Current.Output);
            }
            completedOutputs.Dispose();
        }
        _sessionPool.Return(session);
        return result.output;
    }

    public void Dispose()
    {
        _sessionPool.Dispose();
        _store.Dispose();
    }
}