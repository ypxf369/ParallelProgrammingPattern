using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace 使用PLINQ实现MapReduce模式
{
    class Program
    {
        static void Main(string[] args)
        {
            //我们将该文本分隔成单词，然后借助于Map函数见每个单词拆分为单个字符，最后根据字符值进行分组。Reduce函数最终将序列转换为键值对，其中键是字符，值为该字符在该段文本中被使用过的次数，并且该序列按照被使用次数进行排序。因此，我们能够并行地计算该文本中每个字符出现的次数（这是因为我们使用PLNQ来查询初始数据）
            var q = textToParse.Split(delimiters)
                .AsParallel()
                .MapReduce(
                    i => i.ToLower().ToCharArray(),
                    i => i,
                    i => new[] { new { Char = i.Key, Count = i.Count() } })
                .Where(i => char.IsLetterOrDigit(i.Char))
                .OrderByDescending(i => i.Count);

            foreach (var info in q)
            {
                Console.WriteLine("字符 {0} 在文本中出现了 {1} {2}", info.Char, info.Count, info.Count == 1 ? "time" : "times");
            }
            Console.WriteLine(" --------------------------------- ");

            const string searchPattern = "en";

            //使用PLINQ来过滤并保留只包含搜索模式的单词，然后再计算这些单词在该文本中的使用次数。
            var q2 = textToParse.Split(delimiters)
                .AsParallel()
                .Where(i => i.Contains(searchPattern))
                .MapReduce(i => new[] { i },
                i => i,
                i => new[] { new { Word = i.Key, Count = i.Count() } })
                .OrderByDescending(i => i.Count);

            Console.WriteLine("单词搜索模式 '{0}':", searchPattern);
            foreach (var info in q2)
            {
                Console.WriteLine("{0} 在文本中出现了 {1} {2}", info.Word, info.Count, info.Count == 1 ? "time" : "times");
            }

            int halfLengthWordIndex = textToParse.IndexOf(' ', textToParse.Length / 2);

            using (var sw = File.CreateText("1.txt"))
            {
                sw.Write(textToParse.Substring(0, halfLengthWordIndex));
            }

            using (var sw = File.CreateText("2.txt"))
            {
                sw.Write(textToParse.Substring(halfLengthWordIndex));
            }

            string[] paths = new[] { ".\\" };

            Console.WriteLine(" ------------------------------- ");

            //使用文件I/O。将示例文本保存到磁盘，将其分隔为两个文件。然后我们定义Map函数来根据目录名产生一组字符串，目录名是初始目录中所有文本文件中所有行中的所有单词。然后根据单词第一个字母对这些单词进行分组（过滤掉空字符串），然后使用Reduce来看文本中那个字母作为单词首字母使用次数最多。这样的好处在于我们可以使用Map和Reduce函数的其他实现来轻易修改和分发该程序。并且任然可以使用PLINQ来实现Map和Reduce函数，保证程序容易阅读和维护。
            var q3 = paths.SelectMany(i => Directory.EnumerateFiles(i, "*.txt"))
                .AsParallel()
                .MapReduce(
                    path => File.ReadLines(path).SelectMany(line =>
                        line.Trim(delimiters).Split(delimiters)),
                    word => string.IsNullOrWhiteSpace(word)
                        ? '\t'
                        : word.ToLower()[0], g => new[]
                    {
                        new
                        {
                            FirstLetter = g.Key,
                            Count = g.Count()
                        }
                    })
                .Where(i => char.IsLetterOrDigit(i.FirstLetter))
                .OrderByDescending(i => i.Count);

            Console.WriteLine("文本文件中的单词");

            foreach (var info in q3)
            {
                Console.WriteLine("以字母 '{0}' 开头的单词在文本中出现了 {1} {2}", info.FirstLetter, info.Count, info.Count == 1 ? "time" : "times");
            }

            Console.ReadKey();
        }

        private static readonly char[] delimiters =
            Enumerable.Range(0, 256)
            .Select(i => (char)i)
            .Where(i => !char.IsLetterOrDigit(i))
            .ToArray();

        private const string textToParse =
            @"Call me Ishmael. Some years ago - never mind how long precisely - having little or no money in my purse, and nothing particular to interest me on shore, I thought I would sail about a little and see the watery part of the world. It is a way I have of driving off the spleen,and regulating the circulation. Whenever I find myself growing grim about the mouth; whenever it is a damp,drizzly November in my soul;whenever I find myself involutarily pausing b efore coffin warehouses, and bringing up the rear of every funeral I meet;and especially whenever my hypos get such an upper hand of me,that it requires a strong moral principle to prevent me form deliberately stepping into the street, and methodically knocking people's hats off - hten, I account it high time to get to sea as soon as I can.

          Herman Melville , Moby Dick.";
    }

    static class PLINQExtensions
    {
        public static ParallelQuery<TResult> MapReduce<TSource, TMapped, TKey, TResult>(
            this ParallelQuery<TSource> source,
            Func<TSource, IEnumerable<TMapped>> map,
            Func<TMapped, TKey> keySelector,
            Func<IGrouping<TKey, TMapped>, IEnumerable<TResult>> reduce)
        {
            return source.SelectMany(map)//将初始序列转换为需要应用于Map函数的序列
                .GroupBy(keySelector)//分组，并且使用该键以及GroupBy来产生一个中间键/值序列
                .SelectMany(reduce);//应用Reduce函数来得到结果
        }
    }
}
