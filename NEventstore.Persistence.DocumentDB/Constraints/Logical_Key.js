function LogicalKey_Constraint() {
    var context = getContext();
    var collection = context.getCollection();
    var request = context.getRequest();
    var documentToUpdate = request.getBody();

    HasDuplicates();

    function HasDuplicates(continuation)
    {
        var query = {
            query: "SELECT * FROM Commits c WHERE c.BucketId = @BucketId AND c.StreamId = @StreamId AND c.CommitSequence = @CommitSequence",
            parameters : [
                {
                    name: '@BucketId',
                    value: documentToUpdate.BucketId
                },
                {
                    name: '@StreamId',
                    value: documentToUpdate.StreamId
                },
                {
                    name: '@CommitSequence',
                    value: documentToUpdate.CommitSequence
                }
            ]
        };

        var requestOptions = {
            continuation: continuation
        };

        var isNotDuplicate = collection.queryDocuments(collection.getSelfLink(), query, requestOptions,
            function(err, results) {
                if(err) {
                    throw new Error('Error querying for documents with Logical_Key_Constraint_Failures:' + err.message);
                }

                if (results.length > 0)
                {
                    throw new Error('Commit failed due to Logical_Key_Constraint_Failure');
                }
            }
        );

        if (!isNotDuplicate)
        {
            throw new Error('Timeout when verifying Logical_Key_Constraint');
        }
    }
}
