namespace Xtensive.Orm.Tracing
{
  /// <summary>
  /// Query tracing information gathered when Trace() method is called on a LINQ query.
  /// </summary>
  public class TraceInfo
  {
    /// <summary>
    /// Gets the caller information.
    /// </summary>
    public Caller Caller { get; }

    /// <summary>
    ///   Initializes a new instance of this class.
    /// </summary>
    /// <param name="caller">Caller information.</param>
    public TraceInfo(Caller caller)
    {
      Caller = caller;
    }
  }
}