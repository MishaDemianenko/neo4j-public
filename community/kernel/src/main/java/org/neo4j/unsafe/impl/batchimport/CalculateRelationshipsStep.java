package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

public class CalculateRelationshipsStep extends ProcessorStep<Batch<InputRelationship,RelationshipRecord>>
{
    private final RelationshipStore relationshipStore;
    private long numberOfRelationships;
    private long maxSpecific;

    public CalculateRelationshipsStep( StageControl control, Configuration config, RelationshipStore relationshipStore )
    {
        super( control, "RelationshipCalculator", config, 1 );
        this.relationshipStore = relationshipStore;
    }

    @Override
    protected void process( Batch<InputRelationship,RelationshipRecord> batch, BatchSender sender ) throws Throwable
    {
        int batchSize = batch.input.length;
        InputRelationship inputRelationship = batch.input[batchSize - 1];

        if ( inputRelationship.hasSpecificId() )
        {
            maxSpecific = Math.max( inputRelationship.specificId(), maxSpecific );
        }
        else
        {
            numberOfRelationships += batchSize;
        }
    }

    @Override
    protected void done()
    {
        long highestId = relationshipStore.getHighId() + numberOfRelationships;
        relationshipStore.setHighestPossibleIdInUse( Math.max( highestId, maxSpecific ) );
        super.done();
    }
}
