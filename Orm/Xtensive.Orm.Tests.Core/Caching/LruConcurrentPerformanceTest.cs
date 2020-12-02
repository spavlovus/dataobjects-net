using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BitFaster.Caching.Lru;
using NonBlocking;
using NUnit.Framework;
using Xtensive.Caching;
using Xtensive.Caching.Lru;

namespace Xtensive.Orm.Tests.Core.Caching
{
  [TestFixture]
  [Explicit]
  public class LruConcurrentPerformanceTest
  {
    private static int[] samples;
    private static object[] items;

    private static int itemsNumber = 1_000_000;
    private static int samplesNumber = 2_000_000;

    [Test]
    public void RunTests()
    {
      Initialize();

      // big lru, high hit rate (expected load)
      var lruSize = 20_000;
      Console.WriteLine($@"lruSize {lruSize} itemsNumber {itemsNumber} high hit rate by Zipf law");
      GetOrAddBenchLruFastSystemConcurrentDictionary(lruSize);
      GetOrAddBenchLruFastNonBlockingDictionary(lruSize);
      GetOrAddBenchDataObjectsLRU_Pair(lruSize);

      // small lru, high hit rate
      lruSize = 256;
      Console.WriteLine($@"lruSize {lruSize} itemsNumber {itemsNumber} high hit rate by Zipf");
      GetOrAddBenchLruFastSystemConcurrentDictionary(lruSize);
      GetOrAddBenchLruFastNonBlockingDictionary(lruSize);
      GetOrAddBenchDataObjectsLRU_Pair(lruSize);

      // small lru, low hit rate (current load)
      lruSize = 256;

      var random = new Random();
      for (var i = 0; i < samples.Length; i++) {
        samples[i] = random.Next(0, itemsNumber);
      }

      Console.WriteLine($@"lruSize {lruSize} itemsNumber {itemsNumber} low hit rate - random");
      GetOrAddBenchLruFastSystemConcurrentDictionary(lruSize);
      GetOrAddBenchLruFastNonBlockingDictionary(lruSize);
      GetOrAddBenchDataObjectsLRU_Pair(lruSize);
    }

    public static double GeneralHarmonic(int n, double m)
    {
      var num = 0.0;
      for (var index = 0; index < n; ++index) {
        num += Math.Pow(index + 1, -m);
      }

      return num;
    }

    private static void Initialize()
    {
      samples = new int[samplesNumber];
      items = new object[itemsNumber];
      for (var i = 0; i < items.Length; i++) {
        items[i] = new object();
      }

      var random = new Random();
      var num4 = 1.0 / GeneralHarmonic(itemsNumber, 1);
      Parallel.For(0, samplesNumber, i => {
        int val;
        do {
          val = GetZipfSample(random, itemsNumber, num4);
        } while (val < 0 || val > itemsNumber);

        samples[i] = val;
      });
    }
    
    private static int GetZipfSample(Random rnd, int n, double num2)
    {
      var num1 = 0.0;
      while (num1 == 0.0) {
        num1 = rnd.NextDouble();
      }

      // double num2 = 1.0 / SpecialFunctions.GeneralHarmonic(n, s);
      var num3 = 0.0;
      int num4;
      for (num4 = 1; num4 <= n; ++num4)
      {
        num3 += num2 / (num4);
        if (num3 >= num1) {
          break;
        }
      }
      return num4;
    }

    private static void GetOrAddBenchLruFastSystemConcurrentDictionary(int lruSize)
    {
      var dict = new FastConcurrentLru<object, object>(lruSize,
        (c, p, e) => new SystemConcurrentDictionary<object, LruItem<object, object>>(c, p, e));

      var benchmarkName = "======== GetOrAdd FastConcurrentLru ConcurrentDictionary object->object 1M Ops/sec:";

      void Act(int i, int threadNumber)
      {
        var dummy = dict.GetOrAdd(GetItem(i, threadNumber), j => j);
      }

      WarmUpLru(dict);

      RunBench(benchmarkName, Act);
    }

    private static void WarmUpLru(FastConcurrentLru<object, object> dict)
    {
      for (int i = 0; i < samplesNumber; i++) {
        dict.GetOrAdd(GetItem(i, 0), j => j);
      }
    }

    private static void GetOrAddBenchLruFastNonBlockingDictionary(int lruSize)
    {
      var dict = new FastConcurrentLru<object, object>(lruSize,
        (c, p, e) => new NonBlockingConcurrentDictionary<object, LruItem<object, object>>(c, p, e));

      var benchmarkName = "======== GetOrAdd FastConcurrentLru NonBlockingDictionary object->object 1M Ops/sec:";

      void Act(int i, int threadNumber)
      {
        var dummy = dict.GetOrAdd(GetItem(i, threadNumber), j => j);
      }

      WarmUpLru(dict);

      RunBench(benchmarkName, Act);
    }

    private static void GetOrAddBenchDataObjectsLRU_Pair(int lruSize)
    {
      var dict = new LruCache<object, (object key, object value)>(lruSize, i => i.key);

      var benchmarkName = "======== GetOrAdd DataObjectsLRU Pair object->object 1M Ops/sec:";

      void Act(int i, int threadNumber)
      {
        object val;
        var item = GetItem(i, threadNumber);
        lock (dict) {
          val = dict.TryGetItem(item, true, out var result) ? result.value : null;
        }

        if (val == null) {
          lock (dict) {
            dict.Add((item, item), false);
          }
        }
      }

      for (var i = 0; i < itemsNumber; i++) {
        dict.Add((items[i], items[i]), false);
      }

      RunBench(benchmarkName, Act);
    }

    private static long RunBenchmark(Action<int, int> action, int threads, int time)
    {
      var cnt = new Counter64();
      var workers = new Task[threads];
      var sw = Stopwatch.StartNew();
      var e = new ManualResetEventSlim();
      long stopTime = 0;

      Func<int, Action> createBody = worker => () => {
        var iteration = 0;
        e.Wait();
        while (sw.ElapsedMilliseconds < stopTime) {
          const int batch = 10000;
          for (int i = 0; i < batch; i++) {
            action(iteration++, worker);
          }

          cnt.Add(batch);
        }
      };

      for (int i = 0; i < workers.Length; i++) {
        var threanDumber = i;
        workers[i] = Task.Factory.StartNew(createBody(threanDumber), TaskCreationOptions.LongRunning);
      }

      stopTime = sw.ElapsedMilliseconds + time;
      e.Set();

      Task.WaitAll(workers);
      return cnt.Value;
    }

    private static void RunBench(string benchmarkName, Action<int, int> action)
    {
      Console.WriteLine(benchmarkName);
      var maxThreads = Environment.ProcessorCount;
      for (int i = 1; i <= maxThreads; i++) {
        var mOps = RunBenchmark(action, i, 2000) / 2000000.0;
        if (mOps > 1000) {
          Console.Write(@"{0:f0} ", mOps);
        }
        else if (mOps > 100) {
          Console.Write(@"{0:f1} ", mOps);
        }
        else if (mOps > 10) {
          Console.Write(@"{0:f2} ", mOps);
        }
        else if (mOps > 1) {
          Console.Write(@"{0:f3} ", mOps);
        }
        else {
          Console.Write(@"{0:f4} ", mOps);
        }
      }

      Console.WriteLine();
      GC.Collect();
      GC.Collect();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static object GetItem(int i, int threadNumber) =>
      items[samples[(i + 1) * threadNumber % samplesNumber]];
  }

}
