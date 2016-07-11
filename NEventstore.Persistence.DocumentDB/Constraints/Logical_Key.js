function LogicalKey_Constraint() {
    var context = getContext();
    var collection = context.getCollection();
    var request = context.getRequest();
    var docToUpdate = request.getBody();

    HasDuplicates();

    function HasDuplicates(continuation)
    {
        var query = {
            query: "SELECT * FROM Commits c WHERE c.BucketId = @BucketId AND c.StreamId = @StreamId AND c.CommitSequence = @CommitSequence",
            parameters : [
                {
                    name: '@BucketId',
                    value: docToUpdate.BucketId
                },
                {
                    name: '@StreamId',
                    value: docToUpdate.StreamId
                },
                {
                    name: '@CommitSequence',
                    value: docToUpdate.CommitSequence
                }
            ]
        };

        var requestOptions = {
            continuation: continuation
        };

        var isNotDuplicate = collection.queryDocuments(collection.getSelfLink(), query, requestOptions,
            function(err, results, responseOptions) {
                if(err) {
                    throw new Error('Error querying for documents with Logical_Key_Constraint_Failures:' + err.message);
                }

                if (results.length > 0)
                {
                    throw new Error('Commit failed due to Logical_Key_Constraint_Failure');
                }
                else if(responseOptions.continuation){
                    HasDuplicates(responseOptions.continuation);
                }
                else{
                    // No-Op, commit can proceed
                }
            }
        );

        if (!isNotDuplicate)
        {
            throw new Error('Timeout when verifying Logical_Key_Constraint');
        }
    }
}
