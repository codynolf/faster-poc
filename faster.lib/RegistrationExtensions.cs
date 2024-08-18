using faster.lib.options;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace faster.lib;

public static class RegistrationExtensions
{
  public static IServiceCollection AddFasterKV(this IServiceCollection services, IConfiguration configuration)
  {
    var config = configuration.GetSection("FasterKvOptions");
    services.Configure<FasterKvOptions>(configuration.GetSection("FasterKvOptions"));
    services.AddSingleton<IFasterKvService<string, string>, FasterKvService<string, string>>();
    return services;
  }
}
