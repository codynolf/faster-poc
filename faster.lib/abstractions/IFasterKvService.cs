using FASTER.core;

namespace faster.lib;

public interface IFasterKvService<K, V>
    {
        void Dispose();

        V Read(K key);
        ValueTask<(Status, V)> ReadAsync(K key);

        void Upsert(K key, V value);
        ValueTask UpsertAsync(K key, V value);

        void RMW(K key, V value);
        ValueTask RMWAsync(K key, V value);
    }
