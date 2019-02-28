using FluentNHibernate.Mapping;
using System;
using Xunit;

namespace DevExtreme.AspNet.Data.Tests.NH {

    public class SumOverflow {

        public class DataItem : SumOverflowHelper.IEntity {
            public virtual int Id { get; set; }
            public virtual byte? ByteProp { get; set; }
            public virtual short? Int16Prop { get; set; }
            public virtual int? Int32Prop { get; set; }
            public virtual float? SingleProp { get; set; }
        }

        public class DataItemMap : ClassMap<DataItem> {
            public DataItemMap() {
                Table(nameof(SumOverflow) + "_" + nameof(DataItem));
                Id(i => i.Id);
                Map(i => i.ByteProp);
                Map(i => i.Int16Prop);
                Map(i => i.Int32Prop);
                Map(i => i.SingleProp);
            }
        }

        [Fact]
        public void Scenario() {
            SessionFactoryHelper.Exec(session => {
                using(var tx = session.BeginTransaction()) {
                    foreach(var i in SumOverflowHelper.GenerateTestData(() => new DataItem()))
                        session.Save(i);
                    tx.Commit();
                }

                SumOverflowHelper.Run(session.Query<DataItem>());
            });
        }

    }

}
