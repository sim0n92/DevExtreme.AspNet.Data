using System;
using System.Linq;
using System.Collections.Generic;

namespace DevExtreme.AspNet.Data.Async {
    using RegisteredAdapters = Dictionary<Type, Tuple<Func<Type, bool>, IAsyncAdapter>>;

    public static class CustomAsyncAdapters {
        static readonly RegisteredAdapters _registeredAdapters = new RegisteredAdapters();

        public static void RegisterAdapter(Type t,Func<Type, bool> queryProviderTypePredicate, IAsyncAdapter adapter) {
            if(!_registeredAdapters.ContainsKey(t)) {
                _registeredAdapters.Add(t, Tuple.Create(queryProviderTypePredicate, adapter));
            }
        }

        public static void RegisterAdapter(Type queryProviderType, IAsyncAdapter adapter) {
            RegisterAdapter(queryProviderType, type => queryProviderType.IsAssignableFrom(type), adapter);
        }

        internal static IAsyncAdapter GetAdapter(Type queryProviderType) {
            foreach(var i in _registeredAdapters) {
                if(i.Value.Item1(queryProviderType))
                    return i.Value.Item2;
            }
            return null;
        }

#if DEBUG
        internal static void Clear() {
            _registeredAdapters.Clear();
        }
#endif
    }

}
