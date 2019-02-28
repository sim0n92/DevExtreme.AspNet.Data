using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace DevExtreme.AspNet.Data.Tests {

    public static class SumOverflowHelper {

        public interface IEntity {
            Byte? ByteProp { get; set; }
            Int16? Int16Prop { get; set; }
            Int32? Int32Prop { get; set; }
            Single? SingleProp { get; set; }
        }

        public static IEnumerable<T> GenerateTestData<T>(Func<T> itemFactory) where T : IEntity {
            for(var i = 0; i < 2; i++) {
                var item = itemFactory();
                item.ByteProp = Byte.MaxValue;
                item.Int16Prop = Int16.MaxValue;
                item.Int32Prop = Int32.MaxValue;
                item.SingleProp = Single.MaxValue;
                yield return item;
            }
        }

        public static void Run<T>(IQueryable<T> data) {
            var loadResult = DataSourceLoader.Load(data, new SampleLoadOptions {
                RemoteGrouping = true,
                TotalSummary = new[] {
                    new SummaryInfo { SummaryType = "sum", Selector = nameof(IEntity.ByteProp) },
                    new SummaryInfo { SummaryType = "sum", Selector = nameof(IEntity.Int16Prop) },
                    new SummaryInfo { SummaryType = "sum", Selector = nameof(IEntity.Int32Prop) },
                    new SummaryInfo { SummaryType = "sum", Selector = nameof(IEntity.SingleProp) }
                }
            });

            var summary = loadResult.summary;

            Assert.Equal(2m * Byte.MaxValue, summary[0]);
            Assert.Equal(2m * Int16.MaxValue, summary[1]);
            Assert.Equal(2m * Int32.MaxValue, summary[2]);
            Assert.Equal(2d * Single.MaxValue, summary[3]);
        }
    }

}
