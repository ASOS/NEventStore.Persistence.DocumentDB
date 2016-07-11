namespace NEventStore.Persistence.DocumentDB
{
    using NEventStore.Logging;

    public class DocumentPersistenceWireup : PersistenceWireup
    {
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof (DocumentPersistenceWireup));

        public DocumentPersistenceWireup(Wireup wireup, string url, string key)
            : this(wireup, url, key, new DocumentPersistenceOptions())
        {}

        public DocumentPersistenceWireup(Wireup wireup, string url, string key, DocumentPersistenceOptions persistenceOptions)
            : base(wireup)
        {
            Logger.Debug(Messages.ConfiguringEngine);
            Container.Register(c => new DocumentPersistenceFactory(url, key, persistenceOptions).Build());
        }
    }
}
