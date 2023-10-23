using System.Diagnostics;
using FASTER.core;

HelloWorld();
//CacheStore();

void CacheStore()
{
    bool useReadCache = false;
    
    // This sample shows the use of FASTER as a cache + key-value store.
    // Keys and values can be structs or classes.
    // Use blittable structs for *much* better performance

    // Create files for storing data
    using var config = new FasterKVSettings<string, string>("fasterkv") { 
        ReadCacheEnabled = useReadCache
        };

    var path = "ClassCache/";
    var log =  Devices.CreateLogDevice(path + "hlog.log");

    // Log for storing serialized objects; needed only for class keys/values
    var objlog = Devices.CreateLogDevice(path + "hlog.obj.log");

    // Define settings for log
    var logSettings = new LogSettings {
        LogDevice = log, 
        ObjectLogDevice = objlog,
        ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null,
        // Uncomment below for low memory footprint demo
        // PageSizeBits = 12, // (4K pages)
        // MemorySizeBits = 20 // (1M memory for main log)
    };

    // Define serializers; otherwise FASTER will use the slower DataContract
    // Needed only for class keys/values
    

    // Create instance of store
    var store = new FasterKV<string, string>(
        size: 1L << 20,
        logSettings: logSettings,
        checkpointSettings: new CheckpointSettings { CheckpointDir = path }
        //config: config
        );

    // Populate the store
    //PopulateStore(store);

    store.Recover();

    // ******
    // Uncomment below to take checkpoint and wait for its completion
    // This is FoldOver - it will flush the log to disk and store checkpoint metadata
    (bool success, Guid token) = store.TakeFullCheckpointAsync(CheckpointType.FoldOver).GetAwaiter().GetResult();

    // ******
    // Uncomment below to copy entire log to disk, but retain tail of log in memory
    store.Log.Flush(true);

    // ******
    // Uncomment below to move entire log to disk and eliminate data from memory as 
    // well. This will serve workload entirely from disk using read cache if enabled.
    // This will *allow* future updates to the store. The in-mem buffer stays allocated.
    // store.Log.FlushAndEvict(true);

    // ******
    // Uncomment below to move entire log to disk and eliminate data from memory as 
    // well. This will serve workload entirely from disk using read cache if enabled.
    // This will *prevent* future updates to the store. The in-mem buffer is no longer 
    // allocated.
    // store.Log.DisposeFromMemory();

    Console.Write("Enter read workload type (0 = random reads; 1 = interactive): ");
    //var workload = int.Parse(Console.ReadLine());

    //if (workload == 0)
        RandomReadWorkload(store, 1000000);
    //else
    //    InteractiveReadWorkload(store);

    // Clean up
    store.Dispose();
    log.Dispose();
    objlog.Dispose();

    // Delete the created files
    //try { new DirectoryInfo(path).Delete(true); } catch { }
}

void PopulateStore(FasterKV<string, string> store)
{
    // Start session with FASTER
    using var s = store.For(new SimpleFunctions<string, string>()).NewSession<SimpleFunctions<string,string>>();
    Console.WriteLine("Writing keys from 0 to {0} to FASTER", 1000000);

    Stopwatch sw = new();
    sw.Start();
    for (int i = 0; i < 1000000; i++)
    {
        if (i % (1 << 19) == 0)
        {
            GC.Collect();
            long workingSet = Process.GetCurrentProcess().WorkingSet64;
            Console.WriteLine($"{i}: {workingSet / 1048576}M");
        }
        var key = i.ToString();
        var value = i.ToString();
        s.Upsert(ref key, ref value);
    }
    sw.Stop();
    Console.WriteLine("Total time to upsert {0} elements: {1:0.000} secs ({2:0.00} inserts/sec)", 1000000, sw.ElapsedMilliseconds / 1000.0, 1000000 / (sw.ElapsedMilliseconds / 1000.0));
}

void RandomReadWorkload(FasterKV<string, string> store, int max)
{
    // Start session with FASTER
    using var s = store.For(new SimpleFunctions<string, string>()).NewSession<SimpleFunctions<string,string>>();

    Console.WriteLine("Issuing uniform random read workload of {0} reads", max);

    var rnd = new Random(0);

    int statusPending = 0;
    var output = string.Empty;
    Stopwatch sw = new();
    sw.Start();

    for (int i = 0; i < max; i++)
    {
        long k = rnd.Next(max);

        var key = k.ToString();
        var status = s.Read(ref key, ref output);

        if (status.IsPending)
        {
            statusPending++;
            if (statusPending % 100 == 0)
                s.CompletePending(false);
            break;
        }
        else if (status.Found)
        {
            if (output != key)
                throw new Exception("Read error!");
        }
        else
            throw new Exception("Error!");
    }
    s.CompletePending(true);
    sw.Stop();
    Console.WriteLine("Total time to read {0} elements: {1:0.000} secs ({2:0.00} reads/sec)", max, sw.ElapsedMilliseconds / 1000.0, max / (sw.ElapsedMilliseconds / 1000.0));
    Console.WriteLine($"Reads completed with PENDING: {statusPending}");
}

void InteractiveReadWorkload(FasterKV<string, string> store)
{
    // Start session with FASTER
    using var s = store.For(new SimpleFunctions<string, string>()).NewSession<SimpleFunctions<string,string>>();

    Console.WriteLine("Issuing interactive read workload");

    // We use context to store and report latency of async operations
    //var context = new CacheContext { type = 1 };

    while (true)
    {
        Console.Write("Enter key (int), -1 to exit: ");
        int k = int.Parse(Console.ReadLine());
        if (k == -1) break;

        var output = default(string);
        var key = k.ToString();

        //context.ticks = Stopwatch.GetTimestamp();
        var status = s.Read(ref key, ref output);
        if (status.IsPending)
        {
            s.CompletePending(true);
        }
        else if (status.Found)
        {
            long ticks = Stopwatch.GetTimestamp();
            if (output != key)
                Console.WriteLine("Sync: Incorrect value {0} found, latency = {1}ms", output, 1000 * (ticks) / (double)Stopwatch.Frequency);
            else
                Console.WriteLine("Sync: Correct value {0} found, latency = {1}ms", output, 1000 * (ticks) / (double)Stopwatch.Frequency);
        }
        else
        {
            long ticks = Stopwatch.GetTimestamp();// - context.ticks;
            Console.WriteLine("Sync: Value not found, latency = {0}ms", new TimeSpan(ticks).TotalMilliseconds);
        }
    }
}

void HelloWorld()
{
    using var config = new FasterKVSettings<long, long>("fasterkv") { TryRecoverLatest = true };

    long key = 1, value = 1, output = 0;

    // Create FasterKV config based on specified base directory path.
    Console.WriteLine($"FasterKV config:\n{config}\n");

    // Create store using specified config
    using var store = new FasterKV<long, long>(config);

    // Create functions for callbacks; we use a standard in-built function in this sample.
    // You can write your own by extending this or FunctionsBase.
    // In this in-built function, read-modify-writes will perform value merges via summation.
    var funcs = new SimpleFunctions<long, long>((a, b) => a + b);

    // Each logical sequence of calls to FASTER is associated with a FASTER session.
    // No concurrency allowed within a single session
    using var session = store.NewSession(funcs);

    if (store.RecoveredVersion == 1) // did not recover
    {
        Console.WriteLine("Clean start; upserting key-value pair");

        // (1) Upsert and read back upserted value
        session.Upsert(ref key, ref value);

        // Take checkpoint so data is persisted for recovery
        Console.WriteLine("Taking full checkpoint");
        store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot);
        store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
    }
    else
    {
        Console.WriteLine($"Recovered store to version {store.RecoveredVersion}");
    }

    // Reads are served back from memory and return synchronously
    var status = session.Read(ref key, ref output);
    if (status.Found && output == value)
        Console.WriteLine("(1) Success!");
    else
        Console.WriteLine("(1) Error!");

    // (2) Force flush record to disk and evict from memory, so that next read is served from disk
    store.Log.FlushAndEvict(true);

    // Reads from disk will return PENDING status, result available via either asynchronous IFunctions callback
    // or on this thread via CompletePendingWithOutputs, shown below
    status = session.Read(ref key, ref output);
    if (status.IsPending)
    {
        session.CompletePendingWithOutputs(out var iter, true);
        while (iter.Next())
        {
            if (iter.Current.Status.Found && iter.Current.Output == value)
                Console.WriteLine("(2) Success!");
            else
                Console.WriteLine("(2) Error!");
        }
        iter.Dispose();
    }
    else
        Console.WriteLine("(2) Error!");

    /// (3) Delete key, read to verify deletion
    session.Delete(ref key);

    status = session.Read(ref key, ref output);
    if (status.Found)
        Console.WriteLine("(3) Error!");
    else
        Console.WriteLine("(3) Success!");

    // (4) Perform two read-modify-writes (summation), verify result
    key = 2;
    long input1 = 25, input2 = 27;

    session.RMW(ref key, ref input1);
    session.RMW(ref key, ref input2);

    status = session.Read(ref key, ref output);

    if (status.Found && output == input1 + input2)
        Console.WriteLine("(4) Success!");
    else
        Console.WriteLine("(4) Error!");


    // (5) Perform TryAdd using RMW and custom IFunctions
    using var tryAddSession = store.NewSession(new TryAddFunctions<long, long>());
    key = 3; input1 = 30; input2 = 31;

    // First TryAdd - success; status should be NOTFOUND (does not already exist)
    var status1 = tryAddSession.RMWAsync(ref key, ref input1).Result;

    // Second TryAdd - failure; status should be OK (already exists)
    var status2 = tryAddSession.RMWAsync(ref key, ref input2).Result;

    // Read, result should be input1 (first TryAdd)
    var status3 = session.Read(ref key, ref output);

    if (!status.Found && status2.Status.Found && status3.Found && output == input1)
        Console.WriteLine("(5) Success!");
    else
        Console.WriteLine("(5) Error!");

}
