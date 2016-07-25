namespace NEventStore.Persistence.DocumentDB
{
    using System;

    internal class DateEpoch
    {
        public DateTime Date { get; set; }
        public long Epoch
        {
            get
            {
                return (this.Date.Equals(null) || this.Date.Equals(DateTime.MinValue))
                    ? int.MinValue
                    : this.Date.ToEpoch();
            }
        }
    }
}