using MongoDB.Driver;
using MongoDB.Driver.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace DevExtreme.AspNet.Data.Async {

    //public static class IMyAsyncCursorSourceExtensions {
    //    public static Task<List<TDocument>> ToListAsync<TDocument>(this IMongoQueryable<TDocument> source, CancellationToken cancellationToken = default(CancellationToken)) {
    //        return IAsyncCursorSourceExtensions.ToListAsync(source, cancellationToken);
    //    }
    //    public static Task<long> CountAsync<TDocument>(this IMongoQueryable<TDocument> source, CancellationToken cancellationToken = default(CancellationToken)) {
    //        return source.CountAsync(cancellationToken);
    //    }
    //}


    internal class ReflectionAsyncAdapter : IAsyncAdapter {
        readonly QueryProviderInfo _providerInfo;

        public static bool SupportsProvider(QueryProviderInfo providerInfo)
            => providerInfo.IsEFCore
            || IsEF6(providerInfo)
            || providerInfo.IsNH
            || providerInfo.IsMongoDB
            || providerInfo.IsXPO;

        static bool IsEF6(QueryProviderInfo providerInfo)
            => providerInfo.IsEFClassic && providerInfo.Version.Major >= 6;

        public ReflectionAsyncAdapter(QueryProviderInfo providerInfo) {
            _providerInfo = providerInfo;
        }

        public Task<int> CountAsync(IQueryProvider provider, Expression expr, CancellationToken cancellationToken) {
            MethodInfo GetCountAsyncMethod() {
                if(_providerInfo.IsEFCore)
                    return EFCoreMethods.CountAsyncMethod;

                if(IsEF6(_providerInfo))
                    return EF6Methods.CountAsyncMethod;

                if(_providerInfo.IsNH)
                    return NHMethods.CountAsyncMethod;

                if(_providerInfo.IsXPO)
                    return XpoMethods.CountAsyncMethod;

                if(_providerInfo.IsMongoDB)
                    return MongoMethods.CountAsyncMethod;

                throw new NotSupportedException();
            }

            return InvokeCountAsync(GetCountAsyncMethod(), provider, expr, cancellationToken);
        }

        public Task<IEnumerable<T>> ToEnumerableAsync<T>(IQueryProvider provider, Expression expr, CancellationToken cancellationToken) {
            if(_providerInfo.IsEFCore)
                return InvokeToListAsync<T>(EFCoreMethods.ToListAsyncMethod, provider, expr, cancellationToken);

            if(IsEF6(_providerInfo))
                return InvokeToListAsync<T>(EF6Methods.ToListAsyncMethod, provider, expr, cancellationToken);

            if(_providerInfo.IsNH)
                return InvokeToListAsync<T>(NHMethods.ToListAsyncMethod, provider, expr, cancellationToken);

            if(_providerInfo.IsXPO)
                return InvokeToArrayAsync<T>(XpoMethods.ToArrayAsyncMethod, provider, expr, cancellationToken);

            if(_providerInfo.IsMongoDB)
                return InvokeToListAsync<T>(MongoMethods.ToListAsyncMethod, provider, expr, cancellationToken);

            throw new NotSupportedException();
        }

        static class EF6Methods {
            public static readonly MethodInfo CountAsyncMethod;
            public static readonly MethodInfo ToListAsyncMethod;
            static EF6Methods() {
                var extensionsType = Type.GetType("System.Data.Entity.QueryableExtensions, EntityFramework");
                CountAsyncMethod = FindCountAsyncMethod(extensionsType);
                ToListAsyncMethod = FindToListAsyncMethod(extensionsType);
            }
        }

        static class EFCoreMethods {
            public static readonly MethodInfo CountAsyncMethod;
            public static readonly MethodInfo ToListAsyncMethod;
            static EFCoreMethods() {
                var extensionsType = Type.GetType("Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions, Microsoft.EntityFrameworkCore");
                CountAsyncMethod = FindCountAsyncMethod(extensionsType);
                ToListAsyncMethod = FindToListAsyncMethod(extensionsType);
            }
        }

        static class MongoMethods {
            public static readonly MethodInfo CountAsyncMethod;
            public static readonly MethodInfo ToListAsyncMethod;
            static MongoMethods() {
                CountAsyncMethod = FindCountAsyncMethod(typeof(MongoQueryable));
                ToListAsyncMethod = FindToListAsyncMethod(typeof(IAsyncCursorSourceExtensions));
            }
        }

        static class NHMethods {
            public static readonly MethodInfo CountAsyncMethod;
            public static readonly MethodInfo ToListAsyncMethod;
            static NHMethods() {
                var extensionsType = Type.GetType("NHibernate.Linq.LinqExtensionMethods, NHibernate");
                CountAsyncMethod = FindCountAsyncMethod(extensionsType);
                ToListAsyncMethod = FindToListAsyncMethod(extensionsType);
            }
        }

        static class XpoMethods {
            public static readonly MethodInfo CountAsyncMethod;
            public static readonly MethodInfo ToArrayAsyncMethod;
            static XpoMethods() {
                var asm = Array.Find(AppDomain.CurrentDomain.GetAssemblies(), a => a.FullName.StartsWith("DevExpress.Xpo.v"));
                var extensionsType = asm.GetType("DevExpress.Xpo.XPQueryExtensions");
                CountAsyncMethod = FindCountAsyncMethod(extensionsType);
                ToArrayAsyncMethod = FindToArrayAsyncMethod(extensionsType);
            }
        }

        static MethodInfo FindCountAsyncMethod(Type extensionsType) {
            return FindQueryExtensionMethod(extensionsType, "CountAsync");
        }

        static MethodInfo FindToArrayAsyncMethod(Type extensionsType) {
            return FindQueryExtensionMethod(extensionsType, "ToArrayAsync");
        }

        static MethodInfo FindToListAsyncMethod(Type extensionsType) {
            return FindQueryExtensionMethod(extensionsType, "ToListAsync");
        }

        static MethodInfo FindQueryExtensionMethod(Type extensionsType, string name) {
            return extensionsType.GetMethods().First(m => {
                if(!m.IsGenericMethod || m.Name != name)
                    return false;

                var parameters = m.GetParameters();
                return parameters.Length == 2 && parameters[1].ParameterType == typeof(CancellationToken);
            });
        }

        static Task<int> InvokeCountAsync(MethodInfo method, IQueryProvider provider, Expression expr, CancellationToken cancellationToken) {
            var countArgument = ((MethodCallExpression)expr).Arguments[0];
            var query = provider.CreateQuery(countArgument);
            return (Task<int>)InvokeQueryExtensionMethod(method, query.ElementType, query, cancellationToken);
        }

        async static Task<IEnumerable<T>> InvokeToArrayAsync<T>(MethodInfo method, IQueryProvider provider, Expression expr, CancellationToken cancellationToken)
            => await (Task<T[]>)InvokeQueryExtensionMethod(method, typeof(T), provider.CreateQuery(expr), cancellationToken);

        //async static Task<IEnumerable<T>> InvokeToArrayAsyncMongo<T>(MethodInfo method, IQueryProvider provider, Expression expr, CancellationToken cancellationToken)
        //    => await (Task<T[]>)InvokeQueryExtensionMethod(method, new Type[] { typeof(T), typeof(T) }, provider.CreateQuery(expr), cancellationToken);

        async static Task<IEnumerable<T>> InvokeToListAsync<T>(MethodInfo method, IQueryProvider provider, Expression expr, CancellationToken cancellationToken)
            => await (Task<List<T>>)InvokeQueryExtensionMethod(method, typeof(T), provider.CreateQuery(expr), cancellationToken);

        static object InvokeQueryExtensionMethod(MethodInfo method, Type elementType, IQueryable query, CancellationToken cancellationToken) {
            return method
                .MakeGenericMethod(elementType)
                .Invoke(null, new object[] { query, cancellationToken });
        }

        //static object InvokeQueryExtensionMethod(MethodInfo method, Type[] elementType, IQueryable query, CancellationToken cancellationToken) {
        //    return method
        //        .MakeGenericMethod(elementType)
        //        .Invoke(null, new object[] { query, cancellationToken });
        //}

    }

}
