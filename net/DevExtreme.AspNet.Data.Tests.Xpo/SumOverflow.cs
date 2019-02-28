using DevExpress.Xpo;
using System;
using System.Linq;
using Xunit;

namespace DevExtreme.AspNet.Data.Tests.Xpo {

    public class SumOverflow {

        [Persistent(nameof(SumOverflow) + "_" + nameof(DataItem))]
        public class DataItem : SumOverflowHelper.IEntity {
            [Key(AutoGenerate = true)]
            public int ID { get; set; }
            public byte? ByteProp { get; set; }
            public short? Int16Prop { get; set; }
            public int? Int32Prop { get; set; }
            public float? SingleProp { get; set; }
        }

        [Fact]
        public void Scenario() {
            UnitOfWorkHelper.Exec(uow => {
                foreach(var item in SumOverflowHelper.GenerateTestData(() => new DataItem()))
                    uow.Save(item);
                uow.CommitChanges();

                SumOverflowHelper.Run(uow.Query<DataItem>());
            });
        }

    }

}
