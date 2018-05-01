using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace 使用TPL数据流实现并行管道
{
    class Program
    {
        static void Main(string[] args)
        {
            var t = ProcessAsynchronously();
            t.GetAwaiter().GetResult();
        }

        async static Task ProcessAsynchronously()
        {
            var cts = new CancellationTokenSource();

            Task.Run(() =>
            {
                if (Console.ReadKey().KeyChar == 'c')
                {
                    cts.Cancel();
                }
            });

            //定义数据转换步骤
            var inputBlock = new BufferBlock<int>(new DataflowBlockOptions { BoundedCapacity = 5, CancellationToken = cts.Token });

            var filter1Block = new TransformBlock<int, decimal>(i =>
               {
                   decimal result = Convert.ToDecimal(i * 0.97);
                   Console.WriteLine("Filter 1 sent {0} 到下一阶段 on thread id {1}", result, Thread.CurrentThread.ManagedThreadId);
                   Thread.Sleep(TimeSpan.FromMilliseconds(100));
                   return result;
               }, new ExecutionDataflowBlockOptions
               {
                   MaxDegreeOfParallelism = 4,//指定同时运行的最大工作线程数
                   CancellationToken = cts.Token
               });

            var filter2Block = new TransformBlock<decimal, string>(i =>
               {
                   string result = $"--{i}--";
                   Console.WriteLine("Filter 2 sent {0} 到下一阶段 on thread id {1}", result, Thread.CurrentThread.ManagedThreadId);
                   Thread.Sleep(TimeSpan.FromMilliseconds(100));
                   return result;
               }, new ExecutionDataflowBlockOptions
               {
                   MaxDegreeOfParallelism = 4,
                   CancellationToken = cts.Token
               });

            //每个传入的元素运行一个指定的操作
            var outputBlock = new ActionBlock<string>(i =>
              {
                  Console.WriteLine("最终结果 {0} 线程id {1}", i, Thread.CurrentThread.ManagedThreadId);
              },
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 4,
                CancellationToken = cts.Token
            });

            //链接块,PropagateCompletion = true当步骤完成时，将自动将结果和异常传播到下一阶段
            inputBlock.LinkTo(filter1Block, new DataflowLinkOptions { PropagateCompletion = true });
            filter1Block.LinkTo(filter2Block, new DataflowLinkOptions { PropagateCompletion = true });
            filter2Block.LinkTo(outputBlock, new DataflowLinkOptions { PropagateCompletion = true });

            //并行的向缓冲块中添加项
            try
            {
                Parallel.For(0, 20, new ParallelOptions
                {
                    MaxDegreeOfParallelism = 4,
                    CancellationToken = cts.Token
                }, i =>
                {
                    Console.WriteLine("添加 {0} 到数据源 线程id {1}", i,
                        Thread.CurrentThread.ManagedThreadId);
                    inputBlock.SendAsync(i).GetAwaiter().GetResult();
                });
                //添加完成时调用块的Complete方法
                inputBlock.Complete();
                await outputBlock.Completion;
                Console.WriteLine("Press ENTER to exit");
            }
            catch (OperationCanceledException e)
            {
                Console.WriteLine("操作已经被取消！Press ENTER to exit.");
            }
            Console.ReadLine();
        }
    }
}
