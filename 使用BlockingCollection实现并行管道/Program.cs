using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace 使用BlockingCollection实现并行管道
{
    class Program
    {
        //并行管道模式以顺序阶段的方式处理元素
        static void Main(string[] args)
        {
            var cts = new CancellationTokenSource();

            Task.Run(() =>
            {
                if (Console.ReadKey().KeyChar == 'c')
                {
                    cts.Cancel();
                }
            });

            var sourceArrays = new BlockingCollection<int>[CollectionsNumber];
            for (int i = 0; i < sourceArrays.Length; i++)
            {
                //将初始值放入到前四个集合中，其将被作为数据源被下一阶段使用
                sourceArrays[i] = new BlockingCollection<int>(Count);
            }

            var filter1 = new PipelineWorker<int, decimal>(sourceArrays, i => Convert.ToDecimal(i * 0.97), cts.Token, "filter1");

            var filter2 = new PipelineWorker<decimal, string>(filter1.Output, i => string.Format("--{0}--", i), cts.Token, "filter2");

            var filter3 = new PipelineWorker<string, string>(filter2.Output, i => Console.WriteLine("最终结果 {0} 线程 id {1} ", i, Thread.CurrentThread.ManagedThreadId), cts.Token, "filter3");

            try
            {
                Parallel.Invoke(
                    () =>
                    {
                        Parallel.For(0, sourceArrays.Length * Count, (j, state) =>
                        {
                            if (cts.Token.IsCancellationRequested)
                            {
                                state.Stop();
                            }
                            int k = BlockingCollection<int>.TryAddToAny(sourceArrays, j);
                            if (k >= 0)
                            {
                                Console.WriteLine("加入 {0} 到源数据 线程id {1}", j,
                                    Thread.CurrentThread.ManagedThreadId);
                                Thread.Sleep(TimeSpan.FromMilliseconds(100));

                            }
                        });
                        foreach (var arr in sourceArrays)
                        {
                            arr.CompleteAdding();
                        }
                    },
                    () => filter1.Run(),
                    () => filter2.Run(),
                    () => filter3.Run()
                    );
            }
            catch (AggregateException ae)
            {
                foreach (var ex in ae.InnerExceptions)
                {
                    Console.WriteLine(ex.Message + ex.StackTrace);
                }
            }
            if (cts.Token.IsCancellationRequested)
            {
                Console.WriteLine("操作已经被取消! Press ENTER to exit.");
            }
            else
            {
                Console.WriteLine("Press ENTER to exit");
            }
            Console.ReadLine();
        }

        private const int CollectionsNumber = 4;
        private const int Count = 10;

        class PipelineWorker<TInput, TOutput>
        {
            Func<TInput, TOutput> _processor = null;
            Action<TInput> _outputProcessor = null;
            BlockingCollection<TInput>[] _input;
            CancellationToken _token;

            public PipelineWorker(BlockingCollection<TInput>[] input, Func<TInput, TOutput> processor,
                CancellationToken token, string name)
            {
                _input = input;
                Output = new BlockingCollection<TOutput>[_input.Length];
                for (int i = 0; i < Output.Length; i++)
                {
                    Output[i] = null == input[i] ? null : new BlockingCollection<TOutput>(Count);
                }

                _processor = processor;
                _token = token;
                Name = name;
            }

            public PipelineWorker(BlockingCollection<TInput>[] input, Action<TInput> renderer,
                CancellationToken token, string name)
            {
                _input = input;
                _outputProcessor = renderer;
                _token = token;
                Name = name;
                Output = null;
            }

            public BlockingCollection<TOutput>[] Output { get; private set; }
            public string Name { get; private set; }

            public void Run()
            {
                Console.WriteLine("{0} is running", Name);
                while (!_input.All(i => i.IsCompleted) && !_token.IsCancellationRequested)
                {
                    TInput receivedItem;
                    int i = BlockingCollection<TInput>.TryTakeFromAny(_input, out receivedItem, 50, _token);
                    if (i >= 0)
                    {
                        if (Output != null)
                        {
                            TOutput outputItem = _processor(receivedItem);
                            BlockingCollection<TOutput>.AddToAny(Output, outputItem);
                            Console.WriteLine("{0} sent {1} to next,线程id {2}", Name, outputItem,
                                Thread.CurrentThread.ManagedThreadId);
                            Thread.Sleep(TimeSpan.FromMilliseconds(100));
                        }
                        else
                        {
                            _outputProcessor(receivedItem);
                        }
                    }
                    else
                    {
                        Thread.Sleep(TimeSpan.FromMilliseconds(50));
                    }
                }
                if (Output != null)
                {
                    foreach (var bc in Output)
                    {
                        bc.CompleteAdding();
                    }
                }
            }
        }

    }
}
