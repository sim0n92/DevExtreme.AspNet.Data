using System;
using System.Linq;
using Xunit;

namespace DevExtreme.AspNet.Data.Tests.EF6 {
    using DataItem = SumOverflow_DataItem;

    class SumOverflow_DataItem : SumOverflowHelper.IEntity {
        public int ID { get; set; }
        public byte? ByteProp { get; set; }
        public short? Int16Prop { get; set; }
        public int? Int32Prop { get; set; }
        public float? SingleProp { get; set; }
    }

    public class SumOverflow {

        [Fact]
        public void Scenario() {
            TestDbContext.Exec(context => {
                var dbSet = context.Set<DataItem>();

                dbSet.AddRange(SumOverflowHelper.GenerateTestData(() => new DataItem()));
                context.SaveChanges();

                SumOverflowHelper.Run(dbSet);
            });
        }
    }
}
