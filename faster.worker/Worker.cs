using faster.lib;

namespace faster.worker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IFasterKvService<string, string> _fasterKvServiceString;

    public Worker(ILogger<Worker> logger,
                  IFasterKvService<string, string> fasterKvServiceString) 
    {
        _logger = logger;
        _fasterKvServiceString = fasterKvServiceString;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
           _fasterKvServiceString.Upsert("key", "value");

           var read = _fasterKvServiceString.Read("key");

           int bp = 0;
        }
    }
}
