using System;

namespace faster.lib.options;

public class FasterKvOptions
{
  public static string SectionName => "FasterKvOptions";
  public string LogDirectory { get; set; } = null!;
  public string LogFileName { get; set; } = null!;
  public bool TryRecoverLatest { get; set; } = true;
  public bool UseLargeLog { get; set; } = false;
  public bool UseOsReadBuffering { get; set; } = false;
  public bool DeleteOnClose { get; set; } = true;
  public int InitialSize { get; set; } = 1 << 20;
}
