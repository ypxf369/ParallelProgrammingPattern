using System;
using System.Threading;
using System.Threading.Tasks;

namespace 实现惰性求值的共享状态
{
    class Program
    {
        static void Main(string[] args)
        {
            var t = ProcessAsynchronously();
            t.GetAwaiter().GetResult();

            Console.WriteLine("Press ENTER to exit");
            Console.ReadLine();
        }

        static async Task ProcessAsynchronously()
        {
            Task[] tasks = new Task[4];

            var unsafeState = new UnsafeState();
            for (int i = 0; i < 4; i++)
            {
                tasks[i] = Task.Run(() => Worker(unsafeState));
            }

            await Task.WhenAll(tasks);
            Console.WriteLine(" ----------------------------- ");

            var firstState = new DoubleCheckedLocking();
            for (int i = 0; i < 4; i++)
            {
                tasks[i] = Task.Run(() => Worker(firstState));
            }
            await Task.WhenAll(tasks);
            Console.WriteLine(" ----------------------------- ");

            var secondState = new BCLDoubleChecked();//由于LazyInitializer是一个静态类，因此我们无需创建一个类的实例，而Lazy<T>中的例子则需要，因为该模式的性能在某些场景中更好。
            for (int i = 0; i < 4; i++)
            {
                tasks[i] = Task.Run(() => Worker(secondState));
            }
            await Task.WhenAll(tasks);
            Console.WriteLine(" ----------------------------- ");

            var thirdState = new Lazy<ValueToAccess>(Compute);
            for (int i = 0; i < 4; i++)
            {
                tasks[i] = Task.Run(() => Worker(thirdState));
            }
            await Task.WhenAll(tasks);
            Console.WriteLine(" ----------------------------- ");

            var fourthState = new BCLThreadSafeFactory();
            for (int i = 0; i < 4; i++)
            {
                tasks[i] = Task.Run(() => Worker(fourthState));
            }
            await Task.WhenAll(tasks);
            Console.WriteLine(" ----------------------------- ");
        }

        static void Worker(IHasValue state)
        {
            Console.WriteLine("Worker 运行在线程id {0}", Thread.CurrentThread.ManagedThreadId);
            Console.WriteLine("State value: {0}", state.Value.Text);
        }

        static void Worker(Lazy<ValueToAccess> state)
        {
            Console.WriteLine("Worker 运行在线程id {0}", Thread.CurrentThread.ManagedThreadId);
            Console.WriteLine("State value: {0}", state.Value.Text);
        }

        static ValueToAccess Compute()
        {
            Console.WriteLine("该值是在线程id {0} 上构造的", Thread.CurrentThread.ManagedThreadId);
            Thread.Sleep(TimeSpan.FromSeconds(1));
            return new ValueToAccess(string.Format("基于线程id {0}", Thread.CurrentThread.ManagedThreadId));
        }

        class ValueToAccess
        {
            public ValueToAccess(string text)
            {
                Text = text;
            }

            public string Text { get; }
        }

        class UnsafeState : IHasValue
        {
            private ValueToAccess _value;

            public ValueToAccess Value
            {
                get
                {
                    if (_value == null)
                    {
                        _value = Compute();
                    }
                    return _value;
                }
            }
        }

        class DoubleCheckedLocking : IHasValue
        {
            private readonly object _syncRoot = new object();
            private volatile ValueToAccess _value;

            public ValueToAccess Value
            {
                get
                {
                    if (_value == null)
                    {
                        lock (_syncRoot)
                        {
                            if (_value == null)
                            {
                                _value = Compute();
                            }
                        }
                    }
                    return _value;
                }
            }
        }

        class BCLDoubleChecked : IHasValue
        {
            private object _syncRoot = new object();
            private ValueToAccess _value;
            private bool _initialized = false;

            public ValueToAccess Value => LazyInitializer.EnsureInitialized(ref _value, ref _initialized, ref _syncRoot, Compute);//LazyInitializer内部实现了双重锁定模式
        }

        class BCLThreadSafeFactory : IHasValue
        {
            private ValueToAccess _value;
            //LazyInitializer内部实现了双重锁定模式
            public ValueToAccess Value => LazyInitializer.EnsureInitialized(ref _value, Compute);
        }

        interface IHasValue
        {
            ValueToAccess Value { get; }
        }
    }
}
