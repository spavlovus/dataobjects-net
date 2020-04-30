namespace Xtensive.Orm.Tracing
{
  /// <summary>
  /// Gets the caller information.
  /// </summary>
  public class Caller
  {
    /// <summary>
    /// Gets the member name of a class where Trace() method was called.
    /// </summary>
    public string MemberName { get; }

    /// <summary>
    /// Gets the path to the file where Trace() method was called.
    /// </summary>
    public string FilePath { get; }

    /// <summary>
    /// Gets the line number in the file where Trace() method was called.
    /// </summary>
    public int LineNumber { get; }

    /// <summary>
    ///   Initializes a new instance of this class.
    /// </summary>
    /// <param name="memberName">Member name.</param>
    /// <param name="filePath">File path.</param>
    /// <param name="lineNumber">Line number.</param>
    public Caller(string memberName, string filePath, int lineNumber)
    {
      MemberName = memberName;
      FilePath = filePath;
      LineNumber = lineNumber;
    }
  }
}