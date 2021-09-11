// Copyright (C) 2003-2010 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.

using System;
using System.Collections.Generic;

namespace Xtensive.Sql.Compiler
{
  /// <summary>
  /// Represents a <see cref="SqlCompiler"/> compilation results.
  /// </summary>
  public sealed class SqlCompilationResult
  {
    private readonly Node resultNode;
    private readonly string resultText;
    private readonly IReadOnlyDictionary<object, string> parameterNames;
    private volatile int lastResultLength;

    private readonly IReadOnlyDictionary<object, string> placeholderValues;

    /// <inheritdoc/>
    public override string ToString()
    {
      return GetCommandText();
    }

    /// <summary>
    /// Gets the name of the <paramref name="parameter"/> assigned during compilation.
    /// All explicitly named parameters are not searched by this method.
    /// </summary>
    /// <param name="parameter">The parameter.</param>
    /// <returns>Assigned name.</returns>
    public string GetParameterName(object parameter)
    {
      string result;
      if (parameterNames==null || !parameterNames.TryGetValue(parameter, out result))
        throw new InvalidOperationException(string.Format(Strings.ExNameForParameterXIsNotFound, parameter));
      return result;
    }

    /// <summary>
    /// Gets the textual representation of SQL DOM statement compilation.
    /// </summary>
    /// <value>The SQL text command.</value>
    public string GetCommandText()
    {
      if (resultText!=null)
        return resultText;
      string result = PostCompiler.Process(resultNode, new SqlPostCompilerConfiguration(), lastResultLength, placeholderValues);
      lastResultLength = result.Length;
      return result;
    }

    /// <summary>
    /// Gets the textual representation of SQL DOM statement compilation.
    /// Query is postprocessed using the specified <paramref name="configuration"/>.
    /// </summary>
    /// <param name="configuration">The postcompiler configuration.</param>
    /// <returns>The SQL text command.</returns>
    public string GetCommandText(SqlPostCompilerConfiguration configuration)
    {
      if (resultText!=null)
        return resultText;
      string result = PostCompiler.Process(resultNode, configuration, lastResultLength, placeholderValues);
      lastResultLength = result.Length;
      return result;
    }


    // Constructors

    internal SqlCompilationResult(Node result, IReadOnlyDictionary<object, string> parameterNames, IReadOnlyDictionary<object, string> placeholderValues)
    {
      if (result==null) {
        resultText = string.Empty;
        return;
      }
      var textNode = result as TextNode;
      if (textNode!=null && textNode.Next==null)
        resultText = textNode.Text;
      else
        resultNode = result;
      this.parameterNames = parameterNames.Count > 0 ? parameterNames : null;
      this.placeholderValues = placeholderValues;
    }
  }
}
