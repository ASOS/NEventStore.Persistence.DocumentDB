namespace NEventStore.Persistence.DocumentDB.Constraints
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    public partial class DocumentDbUniqueConstraint
    {
        protected readonly IReadOnlyCollection<string> propertyNames;

        public DocumentDbUniqueConstraint(IReadOnlyCollection<string> propertyNames)
        {
            this.propertyNames = propertyNames;
        }

        public string ConcatenatePropertyNames()
        {
            var stringBuilder = new StringBuilder("\"__uniqueConstraint\"");
            this.propertyNames.ToList().ForEach(s => stringBuilder.AppendFormat(@" + ""_"" + doc[""{0}""]", s));
            return stringBuilder.ToString();
        }
    }
}
