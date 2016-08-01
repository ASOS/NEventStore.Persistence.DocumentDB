namespace NEventStore.Persistence.DocumentDB.Tests
{
    using System.Diagnostics;

    using NEventStore.Persistence.DocumentDB.Constraints;

    using Xunit;
    using Xunit.Should;

    public class DocumentDbUniqueConstraintTests
    {
        [Fact]
        public void ShouldConcatenatePropertiesCorrectly()
        {
            var constraint = new DocumentDbUniqueConstraint(new []{ "PropertyOne", "PropertyTwo", "PropertyThree" });

            var result = constraint.ConcatenatePropertyNames();
            

            result.ShouldBe("\"__uniqueConstraint\" + \"_\" + doc[\"PropertyOne\"] + \"_\" + doc[\"PropertyTwo\"] + \"_\" + doc[\"PropertyThree\"]");

            Trace.WriteLine(constraint.TransformText());
        }
    }
}
