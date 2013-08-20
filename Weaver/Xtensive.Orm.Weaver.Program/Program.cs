﻿// Copyright (C) 2013 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Denis Krjuchkov
// Created:    2013.08.19

using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Reflection;

namespace Xtensive.Orm.Weaver
{
  internal sealed class Program
  {
    private static readonly string[] OptionPrefixes = new[] {"-", "/", "--"};

    private readonly string[] args;
    private readonly ProcessorConfiguration configuration;

    private static void Main(string[] args)
    {
      try {
        new Program(args).Run();
      }
      catch (Exception exception) {
        Console.Error.WriteLine(exception);
        Environment.Exit(ExitCode.InternalError);
      }
    }

    private void Run()
    {
      if (args.Length==0)
        ShowNoArgumentsErrorAndExit();

      foreach (var arg in args)
        ProcessArgument(arg);

      var stopwatch = Stopwatch.StartNew();

      var processor = CreateProcessor();
      var result = processor.Execute(configuration, new ConsoleMessageWriter());

      Console.WriteLine("completed in {0}", stopwatch.Elapsed.TotalSeconds.ToString("0.00", CultureInfo.InvariantCulture));

      ExitAccordingToResult(result);
    }

    private void ExitAccordingToResult(ActionResult result)
    {
      Environment.Exit(result==ActionResult.Success ? ExitCode.Success : ExitCode.ProcessorError);
    }

    private void ProcessArgument(string argument)
    {
      if (argument.StartsWith("@")) {
        foreach (var line in File.ReadLines(argument.Substring(1)))
          ProcessArgument(line);
        return;
      }

      if (!configuration.Parse(StripOptionPrefix(argument)))
        ShowInvalidArgumentErrorAndExit(argument);
    }

    private string StripOptionPrefix(string argument)
    {
      foreach (var prefix in OptionPrefixes)
        if (argument.StartsWith(prefix))
          return argument.Substring(prefix.Length);
      return argument;
    }

    private void ShowNoArgumentsErrorAndExit()
    {
      Console.Error.WriteLine("No arguments specified");
      Environment.Exit(ExitCode.InvalidCommandLine);
    }

    private void ShowInvalidArgumentErrorAndExit(string argument)
    {
      Console.Error.WriteLine("Invalid argument: {0}", argument);
      Environment.Exit(ExitCode.InvalidCommandLine);
    }

    private static IAssemblyProcessor CreateProcessor()
    {
      var weaverAssembly = string.Format(
        "Xtensive.Orm.Weaver.Engine, Version={0}, Culture=neutral, PublicKeyToken={1}",
        ThisAssembly.Version,
        ThisAssembly.PublicKeyToken);

      const string weaverType = "Xtensive.Orm.Weaver.AssemblyProcessor";

      var processorAssembly = Assembly.Load(weaverAssembly);
      var processorType = processorAssembly.GetType(weaverType);
      return (IAssemblyProcessor) Activator.CreateInstance(processorType);
    }

    private Program(string[] args)
    {
      this.args = args;
      configuration = new ProcessorConfiguration();
    }
  }
}
