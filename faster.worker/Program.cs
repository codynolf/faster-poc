using faster.worker;
using faster.lib;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();
builder.Services.AddFasterKV(builder.Configuration);

var host = builder.Build();
host.Run();
